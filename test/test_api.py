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
import sys
import os
import django
if sys.platform == 'darwin':
    sys.path.append('/Users/jeffchen/Documents/gitdir/dashboard')
elif sys.platform == 'win32':
    sys.path.append(r'D:\UserData\Documents\GitHub\dashboard')
else:
    sys.path.append('/home/cyh/bigbrother/dashboard')
os.environ["DJANGO_SETTINGS_MODULE"] = "dashboard.settings"
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from trader.utils import *
from trader.utils.read_config import *


class APITest(asynctest.TestCase):
    def setUp(self):
        self.redis_client = redis.StrictRedis(
            host=config.get('REDIS', 'host', fallback='localhost'),
            db=config.getint('REDIS', 'db', fallback=1), decode_responses=True)
        self.redis_client.get("LastTradingDay")
        self.last_trading_day = datetime.datetime.strptime(self.redis_client.get("LastTradingDay"), '%Y%m%d')

    def tearDown(self) -> None:
        self.redis_client.close()

    async def test_get_shfe_data(self):
        self.assertTrue(await update_from_shfe(self.last_trading_day))

    # async def test_get_dce_data(self):
    #     self.assertTrue(await update_from_dce(self.last_trading_day))
    #
    # async def test_get_czce_data(self):
    #     self.assertTrue(await update_from_czce(self.last_trading_day))
    #
    # async def test_get_cffex_data(self):
    #     self.assertTrue(await update_from_cffex(self.last_trading_day))
    #
    # async def test_get_contracts_argument(self):
    #     self.assertTrue(await get_contracts_argument(self.last_trading_day))
