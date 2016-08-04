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

import ujson as json
import aioredis
import datetime

from trader.utils import logger as my_logger
from trader.strategy import BaseModule
from trader.utils.func_container import param_function
from trader.utils.read_config import *

logger = my_logger.get_logger('CTPApi')
HANDLER_TIME_OUT = config.getint('TRADE', 'command_timeout', fallback=10)


class TradeStrategy(BaseModule):
    market_response_format = config.get('MSG_CHANNEL', 'market_response_format')
    trade_response_format = config.get('MSG_CHANNEL', 'trade_response_format')
    request_format = config.get('MSG_CHANNEL', 'request_format')
    __request_id = random.randint(0, 65535)
    __order_ref = random.randint(0, 999)
    inst_ids = ['IF1609', 'cu1608']

    @staticmethod
    async def query_reader(ch: aioredis.Channel, cb: asyncio.Future):
        msg_list = []
        while await ch.wait_message():
            _, msg = await ch.get(encoding='utf-8')
            msg_dict = json.loads(msg)
            msg_list.append(msg_dict)
            if msg_dict['bIsLast'] and not cb.done():
                cb.set_result(msg_list)

    async def start(self):
        # await self.SubscribeMarketData(self.inst_ids)
        rst = await self.query('Instrument')
        print('Instrument=', len(rst))

        rst = await self.query('InstrumentCommissionRate')
        print('InstrumentCommissionRate=', len(rst))

        rst = await self.query('InstrumentMarginRate')
        print('InstrumentMarginRate=', len(rst))

        rst = await self.query('TradingAccount')
        print('TradingAccount=', rst)

        rst = await self.query('InvestorPosition')
        print('InvestorPosition=', rst)

        rst = await self.query('InvestorPositionDetail')
        print('InvestorPositionDetail=', rst)

        rst = await self.query('Order')
        print('Order=', rst)

    async def stop(self):
        # await self.UnSubscribeMarketData(self.inst_ids)
        pass

    def next_order_ref(self):
        self.__order_ref = 1 if self.__order_ref == 999 else self.__request_id + 1
        now = datetime.datetime.now()
        return '{:02}:{:02}:{:02}:{:03}'.format(now.hour, now.minute, now.second, self.__order_ref)

    def next_id(self):
        self.__request_id = 1 if self.__request_id == 65535 else self.__request_id + 1
        return self.__request_id

    async def query(self, query_type, **kwargs):
        sub_client = None
        channel_name1, channel_name2 = None, None
        try:
            sub_client = await aioredis.create_redis(
                (config.get('REDIS', 'host', fallback='localhost'),
                 config.getint('REDIS', 'port', fallback=6379)),
                db=config.getint('REDIS', 'db', fallback=1))
            request_id = self.next_id()
            kwargs['RequestID'] = request_id
            channel_name1 = self.trade_response_format.format('OnRspQry'+query_type, request_id)
            channel_name2 = self.trade_response_format.format('OnRspError', request_id)
            ch1, ch2 = await sub_client.psubscribe(channel_name1, channel_name2)
            cb = self.io_loop.create_future()
            tasks = [
                asyncio.ensure_future(self.query_reader(ch1, cb), loop=self.io_loop),
                asyncio.ensure_future(self.query_reader(ch2, cb), loop=self.io_loop),
            ]
            self.redis_client.publish(self.request_format.format('ReqQry' + query_type), json.dumps(kwargs))
            rst = await asyncio.wait_for(cb, HANDLER_TIME_OUT, loop=self.io_loop)
            await sub_client.punsubscribe(channel_name1, channel_name2)
            sub_client.close()
            await asyncio.wait(tasks, loop=self.io_loop)
            return rst
        except Exception as e:
            logger.error('%s failed: %s', query_type, repr(e), exc_info=True)
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
                asyncio.ensure_future(self.query_reader(ch1, cb), loop=self.io_loop),
                asyncio.ensure_future(self.query_reader(ch2, cb), loop=self.io_loop),
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
                asyncio.ensure_future(self.query_reader(ch1, cb), loop=self.io_loop),
                asyncio.ensure_future(self.query_reader(ch2, cb), loop=self.io_loop),
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

    async def buy(self, inst: str, limit_price: float):
        pass

    async def sell(self, inst: str, limit_price: float):
        pass

    async def sell_short(self, inst: str, limit_price: float):
        pass

    async def buy_cover(self, inst: str, limit_price: float):
        pass

    async def ReqOrderInsert(self, **kwargs):
        """
        InstrumentID 合约
        VolumeTotalOriginal 手数
        LimitPrice 限价
        StopPrice 止损价
        Direction 方向
        CombOffsetFlag 开,平,平昨
        ContingentCondition 触发条件
        TimeCondition 持续时间
        """
        sub_client = None
        channel_name1, channel_name2 = None, None
        try:
            sub_client = await aioredis.create_redis(
                (config.get('REDIS', 'host', fallback='localhost'),
                 config.getint('REDIS', 'port', fallback=6379)),
                db=config.getint('REDIS', 'db', fallback=1))
            request_id = self.next_id()
            order_ref = self.next_order_ref()
            kwargs['nRequestId'] = request_id
            kwargs['OrderRef'] = order_ref
            channel_name1 = self.trade_response_format.format('OnRspOrderInsert', request_id)
            channel_name2 = self.trade_response_format.format('OnRspError', request_id)
            ch1, ch2 = await sub_client.psubscribe(channel_name1, channel_name2)
            cb = self.io_loop.create_future()
            tasks = [
                asyncio.ensure_future(self.query_reader(ch1, cb), loop=self.io_loop),
                asyncio.ensure_future(self.query_reader(ch2, cb), loop=self.io_loop),
            ]
            self.redis_client.publish(self.request_format.format('ReqOrderInsert'), json.dumps(kwargs))
            rst = await asyncio.wait_for(cb, HANDLER_TIME_OUT, loop=self.io_loop)
            await sub_client.punsubscribe(channel_name1, channel_name2)
            sub_client.close()
            await asyncio.wait(tasks, loop=self.io_loop)
            return rst
        except Exception as e:
            logger.error('ReqOrderInsert failed: %s', repr(e), exc_info=True)
            if sub_client and sub_client.in_pubsub and channel_name1:
                await sub_client.unsubscribe(channel_name1, channel_name2)
                sub_client.close()
            return None

    async def ReqOrderAction(self, inst_ids: list):
        pass

    @param_function(channel='MSG:CTP:RSP:MARKET:OnRtnDepthMarketData:*')
    async def OnRtnDepthMarketData(self, channel, tick: dict):
        """
        'PreOpenInterest': 50990,
        'TradingDay': '20160803',
        'SettlementPrice': 1.7976931348623157e+308,
        'AskVolume1': 40,
        'Volume': 11060,
        'LastPrice': 37740,
        'LowestPrice': 37720,
        'ClosePrice': 1.7976931348623157e+308,
        'ActionDay': '20160803',
        'UpdateMillisec': 0,
        'PreClosePrice': 37840,
        'LowerLimitPrice': 35490,
        'OpenInterest': 49460,
        'UpperLimitPrice': 40020,
        'AveragePrice': 189275.7233273056,
        'HighestPrice': 38230,
        'BidVolume1': 10,
        'UpdateTime': '11:03:12',
        'InstrumentID': 'cu1608',
        'PreSettlementPrice': 37760,
        'OpenPrice': 37990,
        'BidPrice1': 37740,
        'Turnover': 2093389500,
        'AskPrice1': 37750
        """
        try:
            inst = channel.split(':')[-1]
            logger.info('inst=%s, tick: %s', inst, tick)
        except Exception as ee:
            logger.error('fresh_formula failed: %s', repr(ee), exc_info=True)

    @param_function(channel='MSG:CTP:RSP:TRADE::OnRtnTrade:*')
    async def OnRtnTrade(self, channel, trade: dict):
        try:
            order_ref = channel.split(':')[-1]
            logger.info('order_ref=%s, tick: %s', order_ref, trade)
        except Exception as ee:
            logger.error('fresh_formula failed: %s', repr(ee), exc_info=True)

    @param_function(channel='MSG:CTP:RSP:TRADE::OnRtnOrder:*')
    async def OnRtnOrder(self, channel, order: dict):
        try:
            order_ref = channel.split(':')[-1]
            logger.info('order_ref=%s, tick: %s', order_ref, order)
        except Exception as ee:
            logger.error('fresh_formula failed: %s', repr(ee), exc_info=True)
