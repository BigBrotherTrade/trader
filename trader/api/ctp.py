#!/usr/bin/env python
#
# Copyright 2016 timercrack
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import asyncio
import random

import redis
import ujson as json
import aioredis

import trader.utils.logger as my_logger
from trader.utils.read_config import *
from trader.utils import msg_reader

logger = my_logger.get_logger('CTPApi')
HANDLER_TIME_OUT = config.getint('TRADE', 'command_timeout', fallback=10)


class CTPApi:
    def __init__(self, io_loop: asyncio.AbstractEventLoop = None):
        self.io_loop = io_loop
        if self.io_loop is None:
            self.io_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.io_loop)
        self.redis_client = redis.StrictRedis(
            host=config.get('REDIS', 'host', fallback='localhost'),
            db=config.getint('REDIS', 'db', fallback=1), decode_responses=True)
        self.market_response_format = config.get('MSG_CHANNEL', 'market_response_format')
        self.trade_response_format = config.get('MSG_CHANNEL', 'trade_response_format')
        self.request_format = config.get('MSG_CHANNEL', 'request_format')
        self.__request_id = random.randint(0, 65535)

    def next_id(self):
        self.__request_id = 1 if self.__request_id == 65535 else self.__request_id + 1
        return self.__request_id

    async def MarketReqUserLogin(self, broker_id: str, user_id: str, password: str):
        sub_client = None
        channel_name1, channel_name2 = None, None
        try:
            sub_client = await aioredis.create_redis(
                (config.get('REDIS', 'host', fallback='localhost'),
                 config.getint('REDIS', 'port', fallback=6379)),
                db=config.getint('REDIS', 'db', fallback=1))
            request_id = self.next_id()
            channel_name1 = self.market_response_format.format('OnRspUserLogin', '*')
            channel_name2 = self.market_response_format.format('OnRspError', request_id)
            ch1, ch2 = await sub_client.psubscribe(channel_name1, channel_name2)
            cb = self.io_loop.create_future()
            tasks = [
                asyncio.ensure_future(msg_reader(ch1, cb), loop=self.io_loop),
                asyncio.ensure_future(msg_reader(ch2, cb), loop=self.io_loop),
            ]
            param_dict = json.dumps({
                'BrokerID': broker_id,
                'UserID': user_id,
                'Password': password,
                'RequestID': request_id,
            })
            self.redis_client.publish(self.request_format.format('MarketReqUserLogin'), param_dict)
            rst = await asyncio.wait_for(cb, HANDLER_TIME_OUT, loop=self.io_loop)
            await sub_client.punsubscribe(channel_name1, channel_name2)
            sub_client.close()
            await asyncio.wait(tasks, loop=self.io_loop)
            return json.loads(rst)
        except Exception as e:
            logger.error('MarketReqUserLogin failed: %s', repr(e), exc_info=True)
            if sub_client and sub_client.in_pubsub and channel_name1:
                await sub_client.unsubscribe(channel_name1, channel_name2)
                sub_client.close()
            return None

    async def SubscribeMarketData(self, inst_ids: list):
        sub_client = None
        channel_name1, channel_name2 = None, None
        try:
            sub_client = await aioredis.create_redis(
                (config.get('REDIS', 'host', fallback='localhost'),
                 config.getint('REDIS', 'port', fallback=6379)),
                db=config.getint('REDIS', 'db', fallback=1))
            channel_name1 = self.market_response_format.format('OnRspSubMarketData', 0)
            channel_name2 = self.market_response_format.format('OnRspError', 0)
            ch1, ch2 = await sub_client.psubscribe(channel_name1, channel_name2)
            cb = self.io_loop.create_future()
            tasks = [
                asyncio.ensure_future(msg_reader(ch1, cb), loop=self.io_loop),
                asyncio.ensure_future(msg_reader(ch2, cb), loop=self.io_loop),
            ]
            self.redis_client.publish(self.request_format.format('SubscribeMarketData'), json.dumps(inst_ids))
            rst = await asyncio.wait_for(cb, HANDLER_TIME_OUT, loop=self.io_loop)
            await sub_client.punsubscribe(channel_name1, channel_name2)
            sub_client.close()
            await asyncio.wait(tasks, loop=self.io_loop)
            return json.loads(rst)
        except Exception as e:
            logger.error('SubscribeMarketData failed: %s', repr(e), exc_info=True)
            if sub_client and sub_client.in_pubsub and channel_name1:
                await sub_client.unsubscribe(channel_name1, channel_name2)
                sub_client.close()
            return None

    async def UnSubscribeMarketData(self, inst_ids: list):
        sub_client = None
        channel_name1, channel_name2 = None, None
        try:
            sub_client = await aioredis.create_redis(
                (config.get('REDIS', 'host', fallback='localhost'),
                 config.getint('REDIS', 'port', fallback=6379)),
                db=config.getint('REDIS', 'db', fallback=1))
            channel_name1 = self.market_response_format.format('OnRspUnSubMarketData', 0)
            channel_name2 = self.market_response_format.format('OnRspError', 0)
            ch1, ch2 = await sub_client.psubscribe(channel_name1, channel_name2)
            cb = self.io_loop.create_future()
            tasks = [
                asyncio.ensure_future(msg_reader(ch1, cb), loop=self.io_loop),
                asyncio.ensure_future(msg_reader(ch2, cb), loop=self.io_loop),
            ]
            self.redis_client.publish(self.request_format.format('UnSubscribeMarketData'), json.dumps(inst_ids))
            rst = await asyncio.wait_for(cb, HANDLER_TIME_OUT, loop=self.io_loop)
            await sub_client.punsubscribe(channel_name1, channel_name2)
            sub_client.close()
            await asyncio.wait(tasks, loop=self.io_loop)
            return json.loads(rst)
        except Exception as e:
            logger.error('SubscribeMarketData failed: %s', repr(e), exc_info=True)
            if sub_client and sub_client.in_pubsub and channel_name1:
                await sub_client.unsubscribe(channel_name1, channel_name2)
                sub_client.close()
            return None

    async def TradeReqUserLogin(self, broker_id: str, user_id: str, password: str):
        sub_client = None
        channel_name1, channel_name2 = None, None
        try:
            sub_client = await aioredis.create_redis(
                (config.get('REDIS', 'host', fallback='localhost'),
                 config.getint('REDIS', 'port', fallback=6379)),
                db=config.getint('REDIS', 'db', fallback=1))
            request_id = self.next_id()
            channel_name1 = self.trade_response_format.format('OnRspUserLogin', '*')
            channel_name2 = self.trade_response_format.format('OnRspError', request_id)
            ch1, ch2 = await sub_client.psubscribe(channel_name1, channel_name2)
            cb = self.io_loop.create_future()
            tasks = [
                asyncio.ensure_future(msg_reader(ch1, cb), loop=self.io_loop),
                asyncio.ensure_future(msg_reader(ch2, cb), loop=self.io_loop),
            ]
            param_dict = json.dumps({
                'BrokerID': broker_id,
                'UserID': user_id,
                'Password': password,
                'RequestID': request_id,
            })
            self.redis_client.publish(self.request_format.format('TradeReqUserLogin'), param_dict)
            rst = await asyncio.wait_for(cb, HANDLER_TIME_OUT, loop=self.io_loop)
            await sub_client.punsubscribe(channel_name1, channel_name2)
            sub_client.close()
            await asyncio.wait(tasks, loop=self.io_loop)
            return json.loads(rst)
        except Exception as e:
            logger.error('TradeReqUserLogin failed: %s', repr(e), exc_info=True)
            if sub_client and sub_client.in_pubsub and channel_name1:
                await sub_client.unsubscribe(channel_name1, channel_name2)
                sub_client.close()
            return None
