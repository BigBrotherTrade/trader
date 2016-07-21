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

from abc import ABCMeta
from functools import wraps


def param_function(**out_kwargs):
    def _rest_handler(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            return func(self, *args, *kwargs)

        for key, value in out_kwargs.items():
            setattr(wrapper, 'arg_{}'.format(key), value)
        setattr(wrapper, 'is_module_function', True)
        return wrapper

    return _rest_handler


class ParamFunctionContainer(object, metaclass=ABCMeta):
    def __init__(self):
        self.module_arg_dict = dict()
        self._collect_all()

    def _collect_all(self):
        for fun_name in dir(self):
            fun = getattr(self, fun_name)
            if hasattr(fun, 'is_module_function'):
                params = dict()
                for arg in dir(fun):
                    if arg.startswith('arg_'):
                        params[arg[4:]] = getattr(fun, arg)
                self.module_arg_dict[fun_name] = params
