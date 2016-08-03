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
from abc import abstractmethod, ABCMeta
import aioredis
import redis

import trader.utils.logger as my_logger
from trader.utils.func_container import ParamFunctionContainer
from trader.utils.read_config import *

logger = my_logger.get_logger('BaseModule')


class BaseModule(ParamFunctionContainer, metaclass=ABCMeta):
    def __init__(self, io_loop: asyncio.AbstractEventLoop = None):
        super().__init__()
        self.io_loop = io_loop or asyncio.get_event_loop()
        self.sub_client = self.io_loop.run_until_complete(
                aioredis.create_redis((config.get('REDIS', 'host', fallback='localhost'),
                                       config.getint('REDIS', 'port', fallback=6379)),
                                      db=config.getint('REDIS', 'db', fallback=1)))
        self.redis_client = redis.StrictRedis(
            host=config.get('REDIS', 'host', fallback='localhost'),
            db=config.getint('REDIS', 'db', fallback=1), decode_responses=True)
        self.initialized = False
        self.sub_tasks = list()
        self.sub_channels = list()
        self.channel_router = dict()
        self._register_channel()

    def _register_channel(self):
        for fun_name, args in self.module_arg_dict.items():
            if 'channel' not in args:
                raise Exception("wrong param_function prototype, need param: 'channel'")
            self.channel_router[args['channel']] = getattr(self, fun_name)

    async def install(self):
        try:
            self.sub_channels = await self.sub_client.psubscribe(*[a['channel'] for a in self.module_arg_dict.values()])
            for channel in self.sub_channels:
                self.sub_tasks.append(asyncio.ensure_future(self._msg_reader(channel), loop=self.io_loop))
            await self.start()
            self.initialized = True
            logger.info('%s plugin installed', type(self).__name__)
        except Exception as e:
            logger.error('%s plugin install failed: %s', type(self).__name__, repr(e), exc_info=True)

    async def uninstall(self):
        try:
            await self.stop()
            await self.sub_client.punsubscribe(*[a['channel'] for a in self.module_arg_dict.values()])
            # await asyncio.wait(self.sub_tasks, loop=self.io_loop)
            self.sub_tasks.clear()
            self.sub_client.close()
            self.initialized = False
            logger.info('%s plugin uninstalled', type(self).__name__)
        except Exception as e:
            logger.error('%s plugin uninstall failed: %s', type(self).__name__, repr(e), exc_info=True)

    async def _msg_reader(self, ch):
        while await ch.wait_message():
            real_channel, msg = await ch.get_json()
            channel = ch.name.decode()
            # logger.debug("%s channel[%s] Got Message:%s", type(self).__name__, channel, msg)
            self.io_loop.create_task(self.channel_router[channel](real_channel.decode(), msg))
        logger.debug('%s quit query_reader!', type(self).__name__)

    @abstractmethod
    async def start(self):
        pass

    @abstractmethod
    async def stop(self):
        pass

    @classmethod
    def run(cls):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        plugin = cls(loop)
        try:
            loop.create_task(plugin.install())
            loop.run_forever()
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logger.error('run failed: %s', repr(e), exc_info=True)
        finally:
            loop.run_until_complete(plugin.uninstall())
        loop.close()
