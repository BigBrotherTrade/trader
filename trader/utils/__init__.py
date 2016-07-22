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
import aioredis


def str_to_number(s):
    try:
        if not isinstance(s, str):
            return s
        return int(s)
    except ValueError:
        return float(s)


async def msg_reader(ch: aioredis.Channel, cb: asyncio.Future):
    while await ch.wait_message():
        _, msg = await ch.get(encoding='utf-8')
        if not cb.done():
            cb.set_result(msg)
