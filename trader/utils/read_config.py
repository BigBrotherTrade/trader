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

from appdirs import AppDirs
import os
import configparser
from trader import version as app_ver

config_example = """# trader configuration file
[MSG_CHANNEL]
request_pattern = MSG:CTP:REQ:*
request_format = MSG:CTP:REQ:{}
trade_response_prefix = MSG:CTP:RSP:TRADE:
trade_response_format = MSG:CTP:RSP:TRADE:{}:{}
market_response_prefix = MSG:CTP:RSP:MARKET:
market_response_format = MSG:CTP:RSP:MARKET:{}:{}

[TRADE]
command_timeout = 5

[REDIS]
host = 127.0.0.1
port = 6379
db = 0
encoding = utf-8

[MYSQL]
host = 127.0.0.1
port = 3306
db = QuantDB
user = root
password =

[LOG]
level = INFO
format = %(asctime)s %(name)s [%(levelname)s] %(message)s
"""

app_dir = AppDirs('trader', False, version=app_ver)
config_file = os.path.join(app_dir.user_config_dir, 'config.ini')
if not os.path.exists(config_file):
    if not os.path.exists(app_dir.user_config_dir):
        os.makedirs(app_dir.user_config_dir)
    with open(config_file, 'wt') as f:
        f.write(config_example)
    print('create config file:', config_file)

config = configparser.ConfigParser(interpolation=None)
config.read(config_file)
