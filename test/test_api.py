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

import asynctest
import redis

from trader.strategy.ctp import CTPTrader

try:
    import ujson as json
except ImportError:
    import json

import trader.utils.logger as my_logger
from trader.utils.read_config import *

logger = my_logger.get_logger('TestApi')


class APITest(asynctest.TestCase):
    loop = None  # make pycharm happy

    def setUp(self):
        self.redis_client = redis.StrictRedis(
            host=config.get('REDIS', 'host', fallback='localhost'),
            db=config.getint('REDIS', 'db', fallback=1), decode_responses=True)
        self.api = CTPTrader(io_loop=self.loop)

    @asynctest.skip(reason='no need')
    async def test_market_login(self):
        rst = await self.api.MarketReqUserLogin(
            broker_id=config.get('sim', 'broker'),
            user_id=config.get('sim', 'investor'),
            password=config.get('sim', 'passwd'),
        )
        logger.info('market_login rst = %s', rst)
        self.assertIsNotNone(rst)
        self.assertNotEqual(rst, 'failed')

    @asynctest.skip(reason='no need')
    async def test_trade_login(self):
        rst = await self.api.TradeReqUserLogin(
            broker_id=config.get('sim', 'broker'),
            user_id=config.get('sim', 'investor'),
            password=config.get('sim', 'passwd'),
        )
        logger.info('trade_login rst = %s', rst)
        self.assertIsNotNone(rst)
        self.assertNotEqual(rst, 'failed')

    async def test_subscribe(self):
        rst = await self.api.SubscribeMarketData(["IF1608"])
        logger.info('test_subscribe rst = %s', rst)
        self.assertIsNotNone(rst)
        self.assertNotEqual(rst, 'failed')

    async def test_unsubscribe(self):
        rst = await self.api.UnSubscribeMarketData(["IF1608"])
        logger.info('test_unsubscribe rst = %s', rst)
        self.assertIsNotNone(rst)
        self.assertNotEqual(rst, 'failed')
