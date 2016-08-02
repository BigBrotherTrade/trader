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

from trader.strategy.brother2 import TradeStrategy
from trader.utils.read_config import *
import trader.utils.logger as my_logger

logger = my_logger.get_logger('main')


def main():
    loop = asyncio.get_event_loop()
    big_brother = None
    try:
        big_brother = TradeStrategy(io_loop=loop)
        loop.run_until_complete(big_brother.install())
        print('Big Brother is watching you!')
        print('used config file:', config_file)
        print('log stored in:', app_dir.user_log_dir)
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    except Exception as ee:
        logger.info('got error: %s', repr(ee), exc_info=True)
    finally:
        big_brother and loop.run_until_complete(big_brother.uninstall())


if __name__ == '__main__':
    main()
