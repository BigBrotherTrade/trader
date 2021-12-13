# coding=utf-8
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
import redis
import ujson as json

import pytz
import time
import datetime
from collections import defaultdict

from croniter import croniter
import asyncio
from abc import abstractmethod, ABCMeta
import aioredis

import trader.utils.logger as my_logger
from trader.utils.func_container import ParamFunctionContainer
from trader.utils.read_config import *

logger = my_logger.get_logger('BaseModule')


class BaseModule(ParamFunctionContainer, metaclass=ABCMeta):
    def __init__(self, io_loop: asyncio.AbstractEventLoop = None):
        super().__init__()
        self.io_loop = io_loop or asyncio.get_running_loop()
        self.redis_client = aioredis.from_url(
            f"redis://{config.get('REDIS', 'host', fallback='localhost')}:"
            f"{config.getint('REDIS', 'port', fallback=6379)}/{config.getint('REDIS', 'db', fallback=1)}",
            decode_responses=True)
        self.raw_redis = redis.StrictRedis(host=config.get('REDIS', 'host', fallback='localhost'),
                                           port=config.getint('REDIS', 'port', fallback=6379),
                                           db=config.getint('REDIS', 'db', fallback=1), decode_responses=True)
        self.sub_client = self.redis_client.pubsub()
        self.initialized = False
        self.sub_tasks = list()
        self.sub_channels = list()
        self.channel_router = dict()
        self.crontab_router = defaultdict(dict)
        self.datetime = None
        self.time = None
        self.loop_time = None

    def _register_param(self):
        self.datetime = datetime.datetime.now().replace(tzinfo=pytz.FixedOffset(480))
        self.time = time.time()
        self.loop_time = self.io_loop.time()
        for fun_name, args in self.module_arg_dict.items():
            if 'crontab' in args:
                key = args['crontab']
                self.crontab_router[key]['func'] = getattr(self, fun_name)
                self.crontab_router[key]['iter'] = croniter(args['crontab'], self.datetime)
                self.crontab_router[key]['handle'] = None
            elif 'channel' in args:
                self.channel_router[args['channel']] = getattr(self, fun_name)

    def _get_next(self, key):
        return self.loop_time + (self.crontab_router[key]['iter'].get_next() - self.time)

    def _call_next(self, key):
        if self.crontab_router[key]['handle'] is not None:
            self.crontab_router[key]['handle'].cancel()
        self.crontab_router[key]['handle'] = self.io_loop.call_at(self._get_next(key), self._call_next, key)
        self.io_loop.create_task(self.crontab_router[key]['func']())

    async def install(self):
        try:
            self._register_param()
            await self.sub_client.psubscribe(*self.channel_router.keys())
            asyncio.run_coroutine_threadsafe(self._msg_reader(), self.io_loop)
            # self.io_loop.create_task(self._msg_reader())
            for key, cron_dict in self.crontab_router.items():
                if cron_dict['handle'] is not None:
                    cron_dict['handle'].cancel()
                cron_dict['handle'] = self.io_loop.call_at(self._get_next(key), self._call_next, key)
            await self.start()
            self.initialized = True
            logger.info('%s plugin installed', type(self).__name__)
        except Exception as e:
            logger.error('%s plugin install failed: %s', type(self).__name__, repr(e), exc_info=True)

    async def uninstall(self):
        try:
            await self.stop()
            await self.sub_client.punsubscribe()
            # await asyncio.wait(self.sub_tasks, loop=self.io_loop)
            self.sub_tasks.clear()
            await self.sub_client.close()
            for key, cron_dict in self.crontab_router.items():
                if self.crontab_router[key]['handle'] is not None:
                    self.crontab_router[key]['handle'].cancel()
                    self.crontab_router[key]['handle'] = None
            self.initialized = False
            logger.info('%s plugin uninstalled', type(self).__name__)
        except Exception as e:
            logger.error('%s plugin uninstall failed: %s', type(self).__name__, repr(e), exc_info=True)

    async def _msg_reader(self):
        # {'type': 'pmessage', 'pattern': 'channel:*', 'channel': 'channel:1', 'data': 'Hello'}
        async for msg in self.sub_client.listen():
            if msg['type'] == 'pmessage':
                channel = msg['channel']
                pattern = msg['pattern']
                data = json.loads(msg['data'])
                # logger.debug("%s channel[%s] Got Message:%s", type(self).__name__, channel, msg)
                self.io_loop.create_task(self.channel_router[pattern](channel, data))
            elif msg['type'] == 'punsubscribe':
                break
        logger.debug('%s quit _msg_reader!', type(self).__name__)

    @abstractmethod
    async def start(self):
        pass

    @abstractmethod
    async def stop(self):
        pass

    @classmethod
    def run(cls):
        print('strategy run(): do nothing')
        # loop = asyncio.new_event_loop()
        # asyncio.set_event_loop(loop)
        # plugin = cls(loop)
        # try:
        #     loop.create_task(plugin.install())
        #     loop.run_forever()
        # except KeyboardInterrupt:
        #     pass
        # except Exception as e:
        #     logger.error('run failed: %s', repr(e), exc_info=True)
        # finally:
        #     loop.run_until_complete(plugin.uninstall())
        # loop.close()
