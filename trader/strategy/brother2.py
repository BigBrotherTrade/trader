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
import asyncio
import random
import re
from collections import defaultdict
import datetime
import math
import pytz
import xml.etree.ElementTree as ET
from decimal import Decimal
from decimal import getcontext

from django.db.models import F, Q, Max, Min
import requests
from bs4 import BeautifulSoup
import numpy as np
import talib
import ujson as json
import aioredis

from trader.utils import logger as my_logger
from trader.strategy import BaseModule
from trader.utils.func_container import param_function
from trader.utils.read_config import *
from trader.utils import ApiStruct
from panel.models import *

logger = my_logger.get_logger('CTPApi')
HANDLER_TIME_OUT = config.getint('TRADE', 'command_timeout', fallback=10)


class TradeStrategy(BaseModule):
    __market_response_format = config.get('MSG_CHANNEL', 'market_response_format')
    __trade_response_format = config.get('MSG_CHANNEL', 'trade_response_format')
    __request_format = config.get('MSG_CHANNEL', 'request_format')
    __request_id = random.randint(0, 65535)
    __order_ref = random.randint(0, 999)
    __inst_ids = list()
    __instruments = defaultdict(dict)
    __current = 0  # 当前动态权益
    __pre_balance = 0  # 静态权益
    __cash = 0  # 可用资金
    __shares = dict()  # { instrument : position }
    __cur_account = None
    __activeOrders = {}  # 未成交委托单
    __waiting_orders = {}
    __cancel_orders = {}
    __initialed = False
    __login_time = None
    # 提合约字母部分 IF1509 -> IF
    __re_extract = re.compile(r'([a-zA-Z]*)(\d+)')
    __last_time = None
    __watch_pos = {}
    __ATR = {}
    __broker = None
    __strategy = None

    def update_order(self, order):
        if order['OrderStatus'] == ApiStruct.OST_NotTouched and \
                        order['OrderSubmitStatus'] == ApiStruct.OSS_InsertSubmitted:
            if self.__activeOrders.get(order.OrderRef) is None:
                #     logger.info(u"OpenOrder=%s", order)
                #     build_order = self.build_order_from_open_order(order, self.getInstrumentTraits())
                #     self._register_order(build_order)
                #     self.update_order(order)
                pass

    def update_account(self, account):
        # 静态权益=上日结算-出金金额+入金金额
        self.__pre_balance = account['PreBalance'] - account['Withdraw'] + account['Deposit']
        # 动态权益=静态权益+平仓盈亏+持仓盈亏-手续费
        self.__current = self.__pre_balance + account['CloseProfit'] + account['PositionProfit'] - account['Commission']
        self.__cash = account['Available']
        self.__cur_account = account
        self.__broker = Broker.objects.get(username=account['AccountID'])
        self.__broker.cash = Decimal(self.__cash)
        self.__broker.current = Decimal(self.__current)
        self.__broker.pre_balance = Decimal(self.__pre_balance)
        self.__broker.save(update_fields=['cash', 'current', 'pre_balance'])
        logger.info("可用资金: {:,.0f} 静态权益: {:,.0f} 动态权益: {:,.0f}".format(self.__cash, self.__pre_balance, self.__current))
        self.__strategy = self.__broker.strategy_set.get(name='大哥2.0')
        self.__inst_ids = [inst.main_code for inst in self.__strategy.instruments.all()]

    def calc_fee(self, trade: dict):
        inst = trade['InstrumentID']
        if inst not in self.__instruments:
            return -1
        action = trade['OffsetFlag']
        price = trade['Price']
        volume = trade['Volume']
        if action == ApiStruct.OF_Open:
            return self.__instruments[inst]['info']['VolumeMultiple'] * price * volume * \
                   self.__instruments[inst]['fee']['OpenRatioByMoney'] + volume * \
                   self.__instruments[inst]['fee']['OpenRatioByVolume']
        elif action == ApiStruct.OF_Close:
            return self.__instruments[inst]['info']['VolumeMultiple'] * price * volume * \
                   self.__instruments[inst]['fee']['CloseRatioByMoney'] + volume * \
                   self.__instruments[inst]['fee']['CloseRatioByVolume']
        elif action == ApiStruct.OF_CloseToday:
            return self.__instruments[inst]['info']['VolumeMultiple'] * price * volume * \
                   self.__instruments[inst]['fee']['CloseTodayRatioByMoney'] + volume * \
                   self.__instruments[inst]['fee']['CloseTodayRatioByVolume']
        else:
            return 0

    def update_position(self):
        for _, pos in self.__shares.items():
            Trade.objects.update_or_create(
                broker=self.__broker, strategy=self.__strategy, exchange=pos['ExchangeID'],
                instrument=pos['InstrumentID'],
                direction=DirectionType.LONG if pos['Direction'] == ApiStruct.D_Buy else DirectionType.SHORT,
                open_time=datetime.datetime.strptime(
                    pos['OpenDate']+'09', '%Y%m%d%H').replace(tzinfo=pytz.FixedOffset(480)),
                defaults={
                    'shares': pos['Volume'], 'filled_shares': pos['Volume'],
                    'avg_entry_price': Decimal(pos['OpenPrice']),
                    'profit': Decimal(pos['PositionProfitByTrade']), 'frozen_margin': Decimal(pos['Margin'])})

    @staticmethod
    async def query_reader(ch: aioredis.Channel, cb: asyncio.Future):
        msg_list = []
        while await ch.wait_message():
            _, msg = await ch.get(encoding='utf-8')
            msg_dict = json.loads(msg)
            if 'empty' in msg_dict:
                if msg_dict['empty'] is False:
                    msg_list.append(msg_dict)
            else:
                msg_list.append(msg_dict)
            if msg_dict['bIsLast'] and not cb.done():
                cb.set_result(msg_list)

    async def start(self):
        # inst_list = await self.query('Instrument')
        # print('len=', len(inst_list))
        # for inst_id in self.__inst_ids:
        #     inst = (await self.query('Instrument', InstrumentID=inst_id))[0]
        #     if inst['IsTrading'] == 0:
        #         continue
        #     self.__shares[inst['InstrumentID']].append(inst)
        #     self.__instruments[inst['InstrumentID']]['info'] = inst
        #     inst_fee = await self.query('InstrumentCommissionRate', InstrumentID=inst['InstrumentID'])
        #     self.__instruments[inst['InstrumentID']]['fee'] = inst_fee[0]
        #     inst_margin = await self.query('InstrumentMarginRate', InstrumentID=inst['InstrumentID'])
        #     self.__instruments[inst['InstrumentID']]['margin'] = inst_margin[0]
        getcontext().prec = 3
        self.async_query('TradingAccount')
        self.async_query('InvestorPositionDetail')
        order_list = await self.query('Order')
        if order_list:
            for order in order_list:
                if order['OrderStatus'] == ApiStruct.OST_NotTouched and \
                                order['OrderSubmitStatus'] == ApiStruct.OSS_InsertSubmitted:
                    self.__activeOrders[order['OrderRef']] = order
            logger.info("未成交订单: %s", self.__activeOrders)
        self.async_query('Trade', InstrumentID='v1701', TradeID='9999caf')
        await self.SubscribeMarketData(self.__inst_ids)

    async def stop(self):
        pass
        await self.UnSubscribeMarketData(self.__inst_ids)

    def next_order_ref(self):
        self.__order_ref = 1 if self.__order_ref == 999 else self.__request_id + 1
        now = datetime.datetime.now()
        return '{:02}{:02}{:02}{:03}{:03}'.format(
            now.hour, now.minute, now.second, int(now.microsecond / 1000), self.__order_ref)

    def next_id(self):
        self.__request_id = 1 if self.__request_id == 65535 else self.__request_id + 1
        return self.__request_id

    def getShares(self, instrument):
        # 这个函数只能处理持有单一方向仓位的情况，若同时持有多空的头寸，返回结果不正确
        shares = 0
        pos_price = 0
        for pos in self.__shares[instrument]:
            pos_price += pos['Volume'] * pos['OpenPrice']
            shares += pos['Volume'] * (-1 if pos['Direction'] == ApiStruct.D_Sell else 1)
        return shares, pos_price / abs(shares), self.__shares[instrument][0]['OpenDate']

    def getPositions(self, inst_id):
        # 这个函数只能处理持有单一方向仓位的情况，若同时持有多空的头寸，返回结果不正确
        return self.__shares[inst_id][0]

    def async_query(self, query_type, **kwargs):
        request_id = self.next_id()
        kwargs['RequestID'] = request_id
        self.redis_client.publish(self.__request_format.format('ReqQry' + query_type), json.dumps(kwargs))

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
            channel_name1 = self.__trade_response_format.format('OnRspQry' + query_type, request_id)
            channel_name2 = self.__trade_response_format.format('OnRspError', request_id)
            ch1, ch2 = await sub_client.psubscribe(channel_name1, channel_name2)
            cb = self.io_loop.create_future()
            tasks = [
                asyncio.ensure_future(self.query_reader(ch1, cb), loop=self.io_loop),
                asyncio.ensure_future(self.query_reader(ch2, cb), loop=self.io_loop),
            ]
            self.redis_client.publish(self.__request_format.format('ReqQry' + query_type), json.dumps(kwargs))
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
            channel_name1 = self.__market_response_format.format('OnRspSubMarketData', 0)
            channel_name2 = self.__market_response_format.format('OnRspError', 0)
            ch1, ch2 = await sub_client.psubscribe(channel_name1, channel_name2)
            cb = self.io_loop.create_future()
            tasks = [
                asyncio.ensure_future(self.query_reader(ch1, cb), loop=self.io_loop),
                asyncio.ensure_future(self.query_reader(ch2, cb), loop=self.io_loop),
            ]
            self.redis_client.publish(self.__request_format.format('SubscribeMarketData'), json.dumps(inst_ids))
            rst = await asyncio.wait_for(cb, HANDLER_TIME_OUT, loop=self.io_loop)
            await sub_client.punsubscribe(channel_name1, channel_name2)
            sub_client.close()
            await asyncio.wait(tasks, loop=self.io_loop)
            return rst
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
            channel_name1 = self.__market_response_format.format('OnRspUnSubMarketData', 0)
            channel_name2 = self.__market_response_format.format('OnRspError', 0)
            ch1, ch2 = await sub_client.psubscribe(channel_name1, channel_name2)
            cb = self.io_loop.create_future()
            tasks = [
                asyncio.ensure_future(self.query_reader(ch1, cb), loop=self.io_loop),
                asyncio.ensure_future(self.query_reader(ch2, cb), loop=self.io_loop),
            ]
            self.redis_client.publish(self.__request_format.format('UnSubscribeMarketData'), json.dumps(inst_ids))
            rst = await asyncio.wait_for(cb, HANDLER_TIME_OUT, loop=self.io_loop)
            await sub_client.punsubscribe(channel_name1, channel_name2)
            sub_client.close()
            await asyncio.wait(tasks, loop=self.io_loop)
            return rst
        except Exception as e:
            logger.error('SubscribeMarketData failed: %s', repr(e), exc_info=True)
            if sub_client and sub_client.in_pubsub and channel_name1:
                await sub_client.unsubscribe(channel_name1, channel_name2)
                sub_client.close()
            return None

    async def buy(self, inst: dict, limit_price: float, volume: int):
        rst = await self.ReqOrderInsert(
            InstrumentID=inst['InstrumentID'],
            VolumeTotalOriginal=volume,
            LimitPrice=limit_price,
            Direction=ApiStruct.D_Buy,  # 买
            CombOffsetFlag=ApiStruct.OF_Open,  # 开
            ContingentCondition=ApiStruct.CC_Immediately,  # 立即
            TimeCondition=ApiStruct.TC_GFD)  # 当日有效
        return rst

    async def sell(self, inst: dict, limit_price: float, volume: int):
        pos_info = self.getPositions(inst['InstrumentID'])
        # 上期所区分平今和平昨
        close_flag = ApiStruct.OF_Close
        if pos_info['OpenDate'] == datetime.datetime.today().strftime('%Y%m%d') and pos_info['ExchangeID'] == 'SHFE':
            close_flag = ApiStruct.OF_CloseToday
        rst = await self.ReqOrderInsert(
            InstrumentID=inst['InstrumentID'],
            VolumeTotalOriginal=volume,
            LimitPrice=limit_price,
            Direction=ApiStruct.D_Sell,  # 卖
            CombOffsetFlag=close_flag,  # 平
            ContingentCondition=ApiStruct.CC_Immediately,  # 立即
            TimeCondition=ApiStruct.TC_GFD)  # 当日有效
        return rst

    async def sell_short(self, inst: dict, limit_price: float, volume: int):
        rst = await self.ReqOrderInsert(
            InstrumentID=inst['InstrumentID'],
            VolumeTotalOriginal=volume,
            LimitPrice=limit_price,
            Direction=ApiStruct.D_Sell,  # 卖
            CombOffsetFlag=ApiStruct.OF_Open,  # 开
            ContingentCondition=ApiStruct.CC_Immediately,  # 立即
            TimeCondition=ApiStruct.TC_GFD)  # 当日有效
        return rst

    async def buy_cover(self, inst: dict, limit_price: float, volume: int):
        pos_info = self.getPositions(inst['InstrumentID'])
        # 上期所区分平今和平昨
        close_flag = ApiStruct.OF_Close
        if pos_info['OpenDate'] == datetime.datetime.today().strftime('%Y%m%d') and pos_info['ExchangeID'] == 'SHFE':
            close_flag = ApiStruct.OF_CloseToday
        rst = await self.ReqOrderInsert(
            InstrumentID=inst['InstrumentID'],
            VolumeTotalOriginal=volume,
            LimitPrice=limit_price,
            Direction=ApiStruct.D_Buy,  # 买
            CombOffsetFlag=close_flag,  # 平
            ContingentCondition=ApiStruct.CC_Immediately,  # 立即
            TimeCondition=ApiStruct.TC_GFD)  # 当日有效
        return rst

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
            channel_name1 = self.__trade_response_format.format('OnRspOrderInsert', request_id)
            channel_name2 = self.__trade_response_format.format('OnRspError', request_id)
            ch1, ch2 = await sub_client.psubscribe(channel_name1, channel_name2)
            cb = self.io_loop.create_future()
            tasks = [
                asyncio.ensure_future(self.query_reader(ch1, cb), loop=self.io_loop),
                asyncio.ensure_future(self.query_reader(ch2, cb), loop=self.io_loop),
            ]
            self.redis_client.publish(self.__request_format.format('ReqOrderInsert'), json.dumps(kwargs))
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

    async def cancel_order(self, order: dict):
        sub_client = None
        channel_name1, channel_name2 = None, None
        try:
            sub_client = await aioredis.create_redis(
                (config.get('REDIS', 'host', fallback='localhost'),
                 config.getint('REDIS', 'port', fallback=6379)),
                db=config.getint('REDIS', 'db', fallback=1))
            request_id = self.next_id()
            order['nRequestId'] = request_id
            channel_name1 = self.__trade_response_format.format('OnRspOrderAction', request_id)
            channel_name2 = self.__trade_response_format.format('OnRspError', request_id)
            ch1, ch2 = await sub_client.psubscribe(channel_name1, channel_name2)
            cb = self.io_loop.create_future()
            tasks = [
                asyncio.ensure_future(self.query_reader(ch1, cb), loop=self.io_loop),
                asyncio.ensure_future(self.query_reader(ch2, cb), loop=self.io_loop),
            ]
            self.redis_client.publish(self.__request_format.format('ReqOrderAction'), json.dumps(order))
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
            tick['UpdateTime'] = datetime.datetime.strptime(tick['UpdateTime'], "%Y%m%d %H:%M:%S:%f")
            logger.info('inst=%s, tick: %s', inst, tick)
        except Exception as ee:
            logger.error('OnRtnDepthMarketData failed: %s', repr(ee), exc_info=True)

    @param_function(channel='MSG:CTP:RSP:TRADE:OnRtnTrade:*')
    async def OnRtnTrade(self, channel, trade: dict):
        try:
            order_ref = channel.split(':')[-1]
            logger.info('order_ref=%s, trade: %s', order_ref, trade)
        except Exception as ee:
            logger.error('OnRtnTrade failed: %s', repr(ee), exc_info=True)

    @param_function(channel='MSG:CTP:RSP:TRADE:OnRtnOrder:*')
    async def OnRtnOrder(self, channel, order: dict):
        try:
            order_ref = channel.split(':')[-1]
            logger.info('order_ref=%s, order: %s', order_ref, order)
        except Exception as ee:
            logger.error('OnRtnOrder failed: %s', repr(ee), exc_info=True)

    @param_function(channel='MSG:CTP:RSP:TRADE:OnRspQryInvestorPositionDetail:*')
    async def OnRspQryInvestorPositionDetail(self, _, pos: dict):
        if pos['Volume'] > 0:
            old_pos = self.__shares.get(pos['InstrumentID'])
            if old_pos is None:
                self.__shares[pos['InstrumentID']] = pos
            else:
                old_pos['OpenPrice'] = (old_pos['OpenPrice'] * old_pos['Volume'] +
                                        pos['OpenPrice'] * pos['Volume']) / (old_pos['Volume'] + pos['Volume'])
                old_pos['Volume'] += pos['Volume']
                old_pos['PositionProfitByTrade'] += pos['PositionProfitByTrade']
                old_pos['Margin'] += pos['Margin']
        if pos['bIsLast']:
            self.update_position()

    @param_function(channel='MSG:CTP:RSP:TRADE:OnRspQryTradingAccount:*')
    async def OnRspQryTradingAccount(self, _, account: dict):
        self.update_account(account)

    @param_function(channel='MSG:CTP:RSP:TRADE:OnRspQryInstrumentMarginRate:*')
    async def OnRspQryInstrumentMarginRate(self, _, margin: dict):
        self.update_account(margin)

    @param_function(channel='MSG:CTP:RSP:TRADE:OnRspQryInstrumentCommissionRate:*')
    async def OnRspQryInstrumentCommissionRate(self, _, fee: dict):
        self.update_account(fee)

    @param_function(channel='MSG:CTP:RSP:TRADE:OnRspQryInstrument:*')
    async def OnRspQryInstrument(self, _, fee: dict):
        self.update_account(fee)

    @param_function(channel='MSG:CTP:RSP:TRADE:OnRtnInstrumentStatus:*')
    async def OnRtnInstrumentStatus(self, channel, status: dict):
        """
{"EnterReason":"1","EnterTime":"10:30:00","ExchangeID":"SHFE","ExchangeInstID":"ru","InstrumentID":"ru","InstrumentStatus":"2","SettlementGroupID":"00000001","TradingSegmentSN":27}
        """
        try:
            product_code = channel.split(':')[-1]
            inst = self.__strategy.instruments.filter(product_code=product_code).first()
            if inst is None:
                return
            now = datetime.datetime.now().replace(tzinfo=pytz.FixedOffset(480))
            if status['InstrumentStatus'] == ApiStruct.IS_AuctionOrdering and 8 <= now.hour <= 9:
                # 判断是否需要开平仓
                pos = Trade.objects.filter(
                    close_time__isnull=True, exchange=inst.exchange,
                    instrument__regex='^{}[0-9]+'.format(product_code), shares__gt=0).first()
                last_bar = MainBar.objects.filter(
                    exchange=inst.exchange, product_code=product_code).order_by('-time').first()
                signal = Signal.objects.filter(
                    strategy=self.__strategy, instrument=inst, trigger_time__date=last_bar.time,
                    processed=False).first()
                if signal is not None:
                    if signal.type == SignalType.ROLLOVER and pos is not None:
                        self.roll_over(pos.instrument, inst.main_code)
                    elif signal.type == SignalType.BUY and pos is None:
                        pass

            elif status['InstrumentStatus'] == ApiStruct.IS_NoTrading:
                logger.info('%s 进入休息时段', product_code)
        except Exception as ee:
            logger.error('OnRtnInstrumentStatus failed: %s', repr(ee), exc_info=True)

    def roll_over(self, old_inst, new_inst):
        pass

    @param_function(crontab='0 16 * * *')
    async def refresh_instrument(self):
        logger.info('更新合约列表..')
        inst_set = defaultdict(set)
        inst_dict = defaultdict(dict)
        regex = re.compile('(.*?)([0-9]+)$')
        inst_list = await self.query('Instrument')
        for inst in inst_list:
            if not inst['empty']:
                if inst['IsTrading'] == 1:
                    inst_set[inst['ProductID']].add(inst['InstrumentID'])
                    inst_dict[inst['ProductID']]['exchange'] = inst['ExchangeID']
                    inst_dict[inst['ProductID']]['product_code'] = inst['ProductID']
                    if 'name' not in inst_dict[inst['ProductID']]:
                        inst_dict[inst['ProductID']]['name'] = regex.match(inst['InstrumentName']).group(1)
        for code, data in inst_dict.items():
            logger.info('更新合约保证金手续费: %s', code)
            inst, _ = Instrument.objects.update_or_create(product_code=code, defaults={
                'exchange': data['exchange'],
                'name': data['name'],
                'all_inst': ','.join(sorted(inst_set[code])),
            })
        logger.info('更新合约列表完成!')

    @param_function(crontab='0 17 * * *')
    async def collect_quote(self):
        """
        各品种的主联合约：计算基差，主联合约复权（新浪）
        资金曲线（ctp）
        各品种换月标志
        各品种开仓价格
        各品种平仓价格
        微信报告
        """
        try:
            day = datetime.datetime.today()
            day = day.replace(tzinfo=pytz.FixedOffset(480))
            if not self.is_trading_day(day):
                logger.info('今日是非交易日, 不计算任何数据。')
                return
            logger.info('每日盘后计算, day: %s, 获取交易所日线数据..', day)
            self.fetch_daily_bar(day, 'SHFE')
            self.fetch_daily_bar(day, 'DCE')
            self.fetch_daily_bar(day, 'CZCE')
            self.fetch_daily_bar(day, 'CFFEX')
            for inst_obj in Instrument.objects.all():
                logger.info('计算连续合约, 交易信号: %s', inst_obj.name)
                self.calc_main_inst(inst_obj, day)
                self.update_inst_margin(inst_obj)
                self.calc_signal(inst_obj, day)
        except Exception as e:
            logger.error('collect_quote failed: %s', e, exc_info=True)
        logger.info('盘后计算完毕!')

    def update_inst_margin(self, inst):
        """
        更新每一个合约的保证金
        """
        pass

    def calc_main_inst(
            self, inst: Instrument,
            day: datetime.datetime = datetime.datetime.today().replace(tzinfo=pytz.FixedOffset(480))):
        """
        [["2016-07-18","2116.000","2212.000","2106.000","2146.000","34"],...]
        """
        updated = False
        if inst.main_code is not None:
            expire_date = self.calc_expire_date(inst.main_code, day)
        else:
            expire_date = day.strftime('%y%m')
        # 条件1: 成交量最大 & (成交量>1万 & 持仓量>1万 or 股指) = 主力合约
        if inst.exchange == ExchangeType.CFFEX:
            check_bar = DailyBar.objects.filter(
                exchange=inst.exchange, code__regex='^{}[0-9]+'.format(inst.product_code),
                expire_date__gte=expire_date,
                time=day).order_by('-volume').first()
        else:
            check_bar = DailyBar.objects.filter(
                exchange=inst.exchange, code__regex='^{}[0-9]+'.format(inst.product_code),
                expire_date__gte=expire_date,
                time=day, volume__gte=10000, open_interest__gte=10000).order_by('-volume').first()
        # 条件2: 不满足条件1但是连续3天成交量最大 = 主力合约
        if check_bar is None:
            check_bars = list(DailyBar.objects.raw(
                "SELECT a.* FROM panel_dailybar a INNER JOIN(SELECT time, max(volume) v, max(open_interest) i "
                "FROM panel_dailybar WHERE EXCHANGE=%s and CODE RLIKE %s GROUP BY time) b ON a.time = b.time "
                "AND a.volume = b.v AND a.open_interest = b.i "
                "where a.exchange=%s and code Rlike %s AND a.time <= %s ORDER BY a.time desc LIMIT 3",
                [inst.exchange, '^{}[0-9]+'.format(inst.product_code)] * 2 + [day.strftime('%y/%m/%d')]))
            if len(set(bar.code for bar in check_bars)) == 1:
                check_bar = check_bars[0]
            else:
                check_bar = None

        # 之前没有主力合约, 取当前成交量最大的作为主力
        if inst.main_code is None:
            if check_bar is None:
                check_bar = DailyBar.objects.filter(
                    exchange=inst.exchange, code__regex='^{}[0-9]+'.format(inst.product_code),
                    expire_date__gte=expire_date, time=day).order_by('-volume', '-open_interest').first()
            inst.main_code = check_bar.code
            inst.change_time = day
            inst.save(update_fields=['main_code', 'change_time'])
            self.store_main_bar(check_bar)
        # 主力合约发生变化, 做换月处理
        elif check_bar is not None and inst.main_code != check_bar.code and check_bar.code > inst.main_code:
            inst.last_main = inst.main_code
            inst.main_code = check_bar.code
            inst.change_time = day
            inst.save(update_fields=['last_main', 'main_code', 'change_time'])
            self.store_main_bar(check_bar)
            self.handle_rollover(inst, check_bar)
            updated = True
        else:
            bar = DailyBar.objects.get(exchange=inst.exchange, code=inst.main_code, time=day)
            # 若当前主力合约当天成交量为0, 需要换下一个合约
            if bar.volume == 0 or bar.open_interest == Decimal(0):
                check_bar = DailyBar.objects.filter(
                    exchange=inst.exchange, code__regex='^{}[0-9]+'.format(inst.product_code),
                    expire_date__gte=expire_date, time=day).order_by('-volume', '-open_interest').first()
                if bar.code != check_bar.code:
                    inst.last_main = inst.main_code
                    inst.main_code = check_bar.code
                    inst.change_time = day
                    inst.save(update_fields=['last_main', 'main_code', 'change_time'])
                    self.store_main_bar(check_bar)
                    self.handle_rollover(inst, check_bar)
                    updated = True
                else:
                    self.store_main_bar(bar)
            else:
                self.store_main_bar(bar)
        return inst.main_code, updated

    @staticmethod
    def handle_rollover(inst: Instrument, new_bar: DailyBar):
        """
        换月处理, 基差=新合约收盘价-旧合约收盘价, 从今日起之前的所有连续合约的OHLC加上基差
        """
        product_code = re.findall('[A-Za-z]+', new_bar.code)[0]
        old_bar = DailyBar.objects.get(exchange=inst.exchange, code=inst.last_main, time=new_bar.time)
        main_bar = MainBar.objects.get(
            exchange=inst.exchange, product_code=product_code, time=new_bar.time)
        basis = new_bar.close - old_bar.close
        main_bar.basis = basis
        basis = float(basis)
        main_bar.save(update_fields=['basis'])
        MainBar.objects.filter(exchange=inst.exchange, product_code=product_code, time__lte=new_bar.time).update(
            open=F('open') + basis, high=F('high') + basis, low=F('low') + basis, close=F('close') + basis)

    @staticmethod
    def store_main_bar(bar: DailyBar):
        MainBar.objects.update_or_create(
            exchange=bar.exchange, product_code=re.findall('[A-Za-z]+', bar.code)[0], time=bar.time, defaults={
                'cur_code': bar.code,
                'open': bar.open,
                'high': bar.high,
                'low': bar.low,
                'close': bar.close,
                'volume': bar.volume,
                'open_interest': bar.open_interest})

    def calc_signal(self, inst: Instrument,
                    day: datetime.datetime = datetime.datetime.today().replace(tzinfo=pytz.FixedOffset(480))):
        if inst not in self.__strategy.instruments.all():
            return
        break_n = self.__strategy.param_set.get(code='BreakPeriod').int_value
        atr_n = self.__strategy.param_set.get(code='AtrPeriod').int_value
        long_n = self.__strategy.param_set.get(code='LongPeriod').int_value
        short_n = self.__strategy.param_set.get(code='ShortPeriod').int_value
        stop_n = self.__strategy.param_set.get(code='StopLoss').int_value
        # risk = self.__strategy.param_set.get(code='Risk').float_value
        last_bars = MainBar.objects.filter(
            exchange=inst.exchange, product_code=inst.product_code, time__lte=day
        ).order_by('time').values_list('open', 'high', 'low', 'close')
        arr = np.array(last_bars, dtype=float)
        close = arr[-1, 3]
        atr = talib.ATR(arr[:, 1], arr[:, 2], arr[:, 3], timeperiod=atr_n)[-1]
        short_trend = talib.SMA(arr[:, 3], timeperiod=short_n)[-1]
        long_trend = talib.SMA(arr[:, 3], timeperiod=long_n)[-1]
        buy_sig = short_trend > long_trend and close >= np.amax(arr[-break_n:, 3])
        sell_sig = short_trend < long_trend and close <= np.amin(arr[-break_n:, 3])
        roll_over = inst.change_time.date() == day.date()
        if roll_over:
            cur_code = inst.last_main
        else:
            cur_code = inst.main_code
        # 查询该品种目前持有的仓位, 条件是开仓时间<=今天, 尚未未平仓或今天以后平仓(回测用)
        pos = Trade.objects.filter(
            Q(close_time__isnull=True) | Q(close_time__gt=day),
            exchange=inst.exchange, instrument=cur_code, shares__gt=0, open_time__lt=day).first()
        signal = None
        signal_value = None
        if pos is not None:
            # 多头持仓
            if pos.direction == DirectionType.LONG:
                hh = float(MainBar.objects.filter(
                    exchange=inst.exchange, product_code=re.findall('[A-Za-z]+', pos.instrument)[0],
                    time__gte=pos.open_time, time__lte=day).aggregate(Max('high'))['high__max'])
                # 多头止损
                if close <= hh - atr * stop_n:
                    signal = SignalType.SELL
                    signal_value = hh - atr * stop_n
                # 多头换月
                elif roll_over:
                    signal = SignalType.ROLLOVER
            # 空头持仓
            else:
                ll = float(MainBar.objects.filter(
                    exchange=inst.exchange, product_code=re.findall('[A-Za-z]+', pos.instrument)[0],
                    time__gte=pos.open_time, time__lte=day).aggregate(Min('low'))['low__min'])
                # 空头止损
                if close >= ll + atr * stop_n:
                    signal = SignalType.BUY_COVER
                    signal_value = ll + atr * stop_n
                # 空头换月
                elif roll_over:
                    signal = SignalType.ROLLOVER
        # 做多
        elif buy_sig:
            signal = SignalType.BUY
            signal_value = atr
        # 做空
        elif sell_sig:
            signal = SignalType.SELL_SHORT
            signal_value = atr
        if signal is not None:
            Signal.objects.update_or_create(
                strategy_id=1, instrument=inst, type=signal, trigger_time=day, defaults={
                    'trigger_value': signal_value, 'priority': PriorityType.Normal, 'processed': False})

    def fetch_daily_bar(self, day: datetime.datetime, market: str):
        error_data = None
        try:
            day_str = day.strftime('%Y%m%d')
            if market == ExchangeType.SHFE:
                rst = requests.get('http://www.shfe.com.cn/data/dailydata/kx/kx{}.dat'.format(day_str))
                rst_json = rst.json()
                for inst_data in rst_json['o_curinstrument']:
                    """
{'OPENINTERESTCHG': -11154, 'CLOSEPRICE': 36640, 'SETTLEMENTPRICE': 36770, 'OPENPRICE': 36990,
'PRESETTLEMENTPRICE': 37080, 'ZD2_CHG': -310, 'DELIVERYMONTH': '1609', 'VOLUME': 51102,
'PRODUCTSORTNO': 10, 'ZD1_CHG': -440, 'OPENINTEREST': 86824, 'ORDERNO': 0, 'PRODUCTNAME': '铜                  ',
'LOWESTPRICE': 36630, 'PRODUCTID': 'cu_f    ', 'HIGHESTPRICE': 37000}
                    """
                    error_data = inst_data
                    if inst_data['DELIVERYMONTH'] == '小计' or inst_data['PRODUCTID'] == '总计':
                        continue
                    if '_' not in inst_data['PRODUCTID']:
                        continue
                    DailyBar.objects.update_or_create(
                        code=inst_data['PRODUCTID'].split('_')[0] + inst_data['DELIVERYMONTH'],
                        exchange=market, time=day, defaults={
                            'expire_date': inst_data['DELIVERYMONTH'],
                            'open': inst_data['OPENPRICE'] if inst_data['OPENPRICE'] else inst_data['CLOSEPRICE'],
                            'high': inst_data['HIGHESTPRICE'] if inst_data['HIGHESTPRICE'] else
                            inst_data['CLOSEPRICE'],
                            'low': inst_data['LOWESTPRICE'] if inst_data['LOWESTPRICE']
                            else inst_data['CLOSEPRICE'],
                            'close': inst_data['CLOSEPRICE'],
                            'volume': inst_data['VOLUME'] if inst_data['VOLUME'] else 0,
                            'open_interest': inst_data['OPENINTEREST'] if inst_data['OPENINTEREST'] else 0})
            elif market == ExchangeType.DCE:
                rst = requests.post('http://www.dce.com.cn/PublicWeb/MainServlet', {
                    'action': 'Pu00011_result', 'Pu00011_Input.trade_date': day_str, 'Pu00011_Input.variety': 'all',
                    'Pu00011_Input.trade_type': 0})
                soup = BeautifulSoup(rst.text, 'lxml')
                for tr in soup.select("tr")[2:-4]:
                    inst_data = list(tr.stripped_strings)
                    error_data = inst_data
                    """
[0'商品名称', 1'交割月份', 2'开盘价', 3'最高价', 4'最低价', 5'收盘价', 6'前结算价', 7'结算价', 8'涨跌', 9'涨跌1', 10'成交量', 11'持仓量', 12'持仓量变化', 13'成交额']
['豆一', '1609', '3,699', '3,705', '3,634', '3,661', '3,714', '3,668', '-53', '-46', '5,746', '5,104', '-976', '21,077.13']
                    """
                    if '小计' in inst_data[0]:
                        continue
                    DailyBar.objects.update_or_create(
                        code=DCE_NAME_CODE[inst_data[0]] + inst_data[1],
                        exchange=market, time=day, defaults={
                            'expire_date': inst_data[1],
                            'open': inst_data[2].replace(',', '') if inst_data[2] != '-' else
                            inst_data[5].replace(',', ''),
                            'high': inst_data[3].replace(',', '') if inst_data[3] != '-' else
                            inst_data[5].replace(',', ''),
                            'low': inst_data[4].replace(',', '') if inst_data[4] != '-' else
                            inst_data[5].replace(',', ''),
                            'close': inst_data[5].replace(',', ''),
                            'volume': inst_data[10].replace(',', ''),
                            'open_interest': inst_data[11].replace(',', '')})
            elif market == ExchangeType.CZCE:
                rst = requests.get(
                    'http://www.czce.com.cn/portal/DFSStaticFiles/Future/{}/{}/FutureDataDaily.txt'.format(
                        day.year, day_str))
                if rst.status_code == 404:
                    rst = requests.get(
                        'http://www.czce.com.cn/portal/exchange/{}/datadaily/{}.txt'.format(
                            day.year, day_str))
                for lines in rst.content.decode('gbk').split('\r\n')[1:-3]:
                    if '小计' in lines or '品种' in lines:
                        continue
                    inst_data = [x.strip() for x in lines.split('|' if '|' in lines else ',')]
                    error_data = inst_data
                    """
[0'品种月份', 1'昨结算', 2'今开盘', 3'最高价', 4'最低价', 5'今收盘', 6'今结算', 7'涨跌1', 8'涨跌2', 9'成交量(手)', 10'空盘量', 11'增减量', 12'成交额(万元)', 13'交割结算价']
['CF601', '11,970.00', '11,970.00', '11,970.00', '11,800.00', '11,870.00', '11,905.00', '-100.00',
'-65.00', '13,826', '59,140', '-10,760', '82,305.24', '']
                    """
                    DailyBar.objects.update_or_create(
                        code=inst_data[0],
                        exchange=market, time=day, defaults={
                            'expire_date': self.calc_expire_date(inst_data[0], day),
                            'open': inst_data[2].replace(',', '') if Decimal(inst_data[2].replace(',', '')) > 0.1
                            else inst_data[5].replace(',', ''),
                            'high': inst_data[3].replace(',', '') if Decimal(inst_data[3].replace(',', '')) > 0.1
                            else inst_data[5].replace(',', ''),
                            'low': inst_data[4].replace(',', '') if Decimal(inst_data[4].replace(',', '')) > 0.1
                            else inst_data[5].replace(',', ''),
                            'close': inst_data[5].replace(',', ''),
                            'volume': inst_data[9].replace(',', ''),
                            'open_interest': inst_data[10].replace(',', '')})
            elif market == ExchangeType.CFFEX:
                rst = requests.get('http://www.cffex.com.cn/fzjy/mrhq/{}/index.xml'.format(
                    day.strftime('%Y%m/%d')))
                tree = ET.fromstring(rst.text)
                for inst_data in tree.getchildren():
                    """
<dailydata>
<instrumentid>IC1609</instrumentid>
<tradingday>20160824</tradingday>
<openprice>6336.8</openprice>
<highestprice>6364.4</highestprice>
<lowestprice>6295.6</lowestprice>
<closeprice>6314.2</closeprice>
<openinterest>24703.0</openinterest>
<presettlementprice>6296.6</presettlementprice>
<settlementpriceIF>6317.6</settlementpriceIF>
<settlementprice>6317.6</settlementprice>
<volume>10619</volume>
<turnover>1.3440868E10</turnover>
<productid>IC</productid>
<delta/>
<segma/>
<expiredate>20160919</expiredate>
</dailydata>
                    """
                    error_data = list(inst_data.itertext())
                    DailyBar.objects.update_or_create(
                        code=inst_data.findtext('instrumentid').strip(),
                        exchange=market, time=day, defaults={
                            'expire_date': inst_data.findtext('expiredate')[2:6],
                            'open': inst_data.findtext('openprice').replace(',', '') if inst_data.findtext(
                                'openprice') else inst_data.findtext('closeprice').replace(',', ''),
                            'high': inst_data.findtext('highestprice').replace(',', '') if inst_data.findtext(
                                'highestprice') else inst_data.findtext('closeprice').replace(',', ''),
                            'low': inst_data.findtext('lowestprice').replace(',', '') if inst_data.findtext(
                                'lowestprice') else inst_data.findtext('closeprice').replace(',', ''),
                            'close': inst_data.findtext('closeprice').replace(',', ''),
                            'volume': inst_data.findtext('volume').replace(',', ''),
                            'open_interest': inst_data.findtext('openinterest').replace(',', '')})
            return True
        except Exception as e:
            logger.error('%s, row=%s', repr(e), error_data, exc_info=True)
            return False

    def fetch_today_bars(self):
        day = datetime.datetime.today()
        if self.is_trading_day(day):
            self.fetch_daily_bar(day, 'SHFE')
            self.fetch_daily_bar(day, 'DCE')
            self.fetch_daily_bar(day, 'CZCE')
            self.fetch_daily_bar(day, 'CFFEX')

    @staticmethod
    def is_trading_day(day: datetime.datetime = datetime.datetime.today()):
        """
        判断是否是交易日, 方法是从中金所获取今日的K线数据,判断http的返回码(如果出错会返回302重定向至404页面),
        因为开市前也可能返回302, 所以适合收市后(下午)使用
        :return: bool
        """
        rst = requests.get('http://www.cffex.com.cn/fzjy/mrhq/{}/index.xml'.format(day.strftime('%Y%m/%d')),
                           allow_redirects=False)
        return rst.status_code != 302

    @staticmethod
    def calc_expire_date(inst_code: str, day: datetime.datetime):
        expire_date = int(re.findall('\d+', inst_code)[0])
        if expire_date < 1000:
            year_exact = math.floor(day.year % 100 / 10)
            if expire_date < 100 and day.year % 10 == 9:
                year_exact += 1
            expire_date += year_exact * 1000
        return expire_date

    def create_main(self, inst: Instrument):
        print('processing ', inst.product_code)
        for day in DailyBar.objects.all().order_by('time').values_list('time', flat=True).distinct():
            print(day, self.calc_main_inst(inst, datetime.datetime.combine(
                day, datetime.time.min.replace(tzinfo=pytz.FixedOffset(480)))))

    def create_main_all(self):
        for inst in Instrument.objects.filter(id__gte=15):
            self.create_main(inst)
        print('all done!')

    # def build_order_from_open_order(self, order, instrument_traits):
    #     ret = None
    #     if order.ContingentCondition != ApiStruct.CC_Immediately:
    #         # 创建服务器条件单
    #         if order.LimitPrice < 1:
    #             ret = broker.StopOrder(self.get_order_action(order), order.InstrumentID, order.StopPrice,
    #                                    order.VolumeTotalOriginal, instrument_traits)
    #         else:
    #             ret = broker.StopLimitOrder(self.get_order_action(order), order.InstrumentID, order.StopPrice,
    #                                         order.LimitPrice, order.VolumeTotalOriginal, instrument_traits)
    #     else:
    #         ret = broker.LimitOrder(self.get_order_action(order), order.InstrumentID, order.LimitPrice,
    #                                 order.VolumeTotalOriginal, instrument_traits)
    #     ret.setId(order.OrderRef)
    #     ret.set_ctp_order(copy.copy(order))
    #     if order.OrderStatus == ApiStruct.OST_NoTradeQueueing:
    #         ret.setState(broker.Order.State.INITIAL)
    #     elif order.OrderStatus == ApiStruct.OST_AllTraded:
    #         ret.setState(broker.Order.State.FILLED)
    #     else:
    #         ret.setSubmitted(order.OrderRef, datetime.datetime.now())
    #         ret.setState(broker.Order.State.ACCEPTED)
    #     return ret
    #
    # def build_order_from_trade(self, trade):
    #     ret = broker.LimitOrder(self.get_order_action_from_trade(trade), trade.InstrumentID, trade.Price,
    #                             trade.Volume, self.getInstrumentTraits())
    #     ret.setId(trade.OrderRef)
    #     ret.setSubmitted(trade.OrderRef, datetime.datetime.now())
    #     ret.setState(broker.Order.State.ACCEPTED)
    #     return ret
    #
    # def _on_user_trades(self, trade):
    #     """:param trade : ctp.ApiStruct.Trade """
    #     order = self.__activeOrders.get(trade.OrderRef)
    #     if order is None:
    #         order = self.build_order_from_trade(trade)
    #     fee = self.calc_fee(inst=trade.InstrumentID, price=trade.Price, volume=trade.Volume,
    #                         action=trade.OffsetFlag)
    #     # Update the order.
    #     order_execution_info = \
    #         broker.OrderExecutionInfo(trade.Price, abs(trade.Volume), fee,
    #                                   datetime.datetime.strptime(trade.TradeDate + trade.TradeTime, "%Y%m%d%H:%M:%S"))
    #     order.addExecutionInfo(order_execution_info)
    #     if not order.isActive():
    #         self._unregister_order(order)
    #     # Notify that the order was updated.
    #     if order.isFilled():
    #         event_type = broker.OrderEvent.Type.FILLED
    #         self.notifyOrderEvent(broker.OrderEvent(order, event_type, order_execution_info))
    #         self.refresh_account_balance()
    #
    # def _on_user_order(self, st_order):
    #     act_order = self.__activeOrders.get(st_order.OrderRef)
    #     # 根据收到的是普通单和服务器条件单，两种情况分别处理
    #     # 服务器条件单
    #     if st_order.ContingentCondition != ApiStruct.CC_Immediately:
    #         if st_order.OrderStatus == ApiStruct.OST_Canceled:  # 条件单已撤销
    #             if act_order is not None:
    #                 self._unregister_order(act_order)
    #                 logger.info(u"删除已撤销的服务器条件单,OrderRef=%s", st_order.OrderRef)
    #         elif st_order.OrderStatus == ApiStruct.OST_Touched:  # 条件单已触发
    #             if act_order is not None:
    #                 self._unregister_order(act_order)
    #                 logger.info(u"删除已触发的服务器条件单,OrderRef=%s", st_order.OrderRef)
    #         elif st_order.OrderStatus == ApiStruct.OST_NotTouched:  # 交易所已接收，未触发
    #             if act_order is None:
    #                 act_order = self.build_order_from_open_order(st_order, self.getInstrumentTraits())
    #                 self._register_order(act_order)
    #                 logger.info(u"收到非本策略发出的服务器条件单,OrderRef=%s", st_order.OrderRef)
    #             else:
    #                 act_order.set_ctp_order(copy.copy(st_order))
    #                 logger.info(u"服务器条件单提交成功,OrderRef=%s", st_order.OrderRef)
    #         return
    #     # 普通单
    #     else:
    #         if st_order.OrderStatus == ApiStruct.OST_AllTraded:  # 全部成交
    #             if act_order is None:
    #                 act_order = self.build_order_from_open_order(st_order, self.getInstrumentTraits())
    #                 self._register_order(act_order)
    #                 logger.info(u"收到非本策略发出的已成交单,可能是服务器条件单发起的,OrderRef=%s", st_order.OrderRef)
    #             else:
    #                 pass  # 在on_user_trade里面修改订单状态，这里不需要有任何操作
    #         elif st_order.OrderStatus == ApiStruct.OST_Canceled:  # 撤单
    #             if act_order is not None:
    #                 self._unregister_order(act_order)
    #                 act_order.switchState(broker.Order.State.CANCELED)
    #                 self.notifyOrderEvent(broker.OrderEvent(act_order, broker.OrderEvent.Type.CANCELED,
    #                                                         st_order.StatusMsg.decode('gbk')))
    #                 logger.info(u"移除已撤销的普通单,OrderRef=%s", st_order.OrderRef)
    #             else:
    #                 logger.info(u"收到非本策略发出的已撤销单,OrderRef=%s", st_order.OrderRef)
    #
    #         else:  # 部分成交
    #             if act_order is None:
    #                 act_order = self.build_order_from_open_order(st_order, self.getInstrumentTraits())
    #                 self._register_order(act_order)
    #                 logger.info(u"收到非本策略发出的普通单,OrderRef=%s,status=%s", st_order.OrderRef, st_order.OrderStatus)
    #             else:
    #                 act_order.set_ctp_order(copy.copy(st_order))
    #                 if act_order.getState() == broker.Order.State.INITIAL:
    #                     act_order.switchState(broker.Order.State.SUBMITTED)
    #                 logger.info(u"报单已受理,ref=%s,inst=%s,price=%s,act=%s,status=%s", act_order.getId(),
    #                             act_order.getInstrument(), act_order.getAvgFillPrice(), act_order.getAction(),
    #                             st_order.OrderStatus)
