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

import pytz
from decimal import Decimal

from django.db.models import Q, Max, Min, Sum
import numpy as np
import talib
import ujson as json
import aioredis

from trader.strategy import BaseModule
from trader.utils.func_container import param_function
from trader.utils.read_config import *
from trader.utils import logger as my_logger, calc_main_inst, is_auction_time
from trader.utils import ApiStruct, myround, is_trading_day, update_from_shfe, update_from_dce, \
    update_from_czce, update_from_cffex
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
    __margin = 0  # 占用保证金
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
        self.__pre_balance = Decimal(account['PreBalance']) - Decimal(account['Withdraw']) + \
                             Decimal(account['Deposit'])
        # 动态权益=静态权益+平仓盈亏+持仓盈亏-手续费
        self.__current = self.__pre_balance + Decimal(account['CloseProfit']) + \
            Decimal(account['PositionProfit']) - Decimal(account['Commission'])
        self.__margin = Decimal(account['CurrMargin'])
        self.__cash = Decimal(account['Available'])
        self.__cur_account = account
        self.__broker = Broker.objects.get(username=account['AccountID'])
        self.__broker.cash = self.__cash
        self.__broker.current = self.__current
        self.__broker.pre_balance = self.__pre_balance
        self.__broker.save(update_fields=['cash', 'current', 'pre_balance'])
        logger.info("可用资金: {:,.0f} 静态权益: {:,.0f} 动态权益: {:,.0f}".format(self.__cash, self.__pre_balance, self.__current))
        self.__strategy = self.__broker.strategy_set.get(name='大哥2.0')
        self.__inst_ids = [inst.product_code for inst in self.__strategy.instruments.all()]

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
            inst = Instrument.objects.filter(
                exchange=pos['ExchangeID'], product_code=re.findall('[A-Za-z]+', pos['InstrumentID'])[0]).first()
            Trade.objects.update_or_create(
                broker=self.__broker, strategy=self.__strategy, instrument=inst,
                code=pos['InstrumentID'],
                direction=DirectionType.LONG if pos['Direction'] == ApiStruct.D_Buy else DirectionType.SHORT,
                open_time=datetime.datetime.strptime(
                    pos['OpenDate']+'09', '%Y%m%d%H').replace(tzinfo=pytz.FixedOffset(480)),
                defaults={
                    'shares': pos['Volume'], 'filled_shares': pos['Volume'],
                    'avg_entry_price': Decimal(pos['OpenPrice']),
                    'cost': pos['Volume'] * Decimal(pos['OpenPrice']) * inst.fee_money * inst.volume_multiple
                        + pos['Volume'] * inst.fee_volume,
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
        # await self.query('TradingAccount')
        # await self.update_equity()
        self.async_query('TradingAccount')
        self.async_query('InvestorPositionDetail')
        order_list = await self.query('Order')
        if order_list:
            for order in order_list:
                if order['OrderStatus'] == ApiStruct.OST_NotTouched and \
                                order['OrderSubmitStatus'] == ApiStruct.OSS_InsertSubmitted:
                    self.__activeOrders[order['OrderRef']] = order
            logger.info("未成交订单: %s", self.__activeOrders)
        await self.SubscribeMarketData(self.__inst_ids)

    async def stop(self):
        pass
        # if self.__inst_ids:
        #     await self.UnSubscribeMarketData(self.__inst_ids)

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

    async def buy(self, inst: Instrument, limit_price: Decimal, volume: int):
        rst = await self.ReqOrderInsert(
            InstrumentID=inst.main_code,
            VolumeTotalOriginal=volume,
            LimitPrice=float(limit_price),
            Direction=ApiStruct.D_Buy,  # 买
            CombOffsetFlag=ApiStruct.OF_Open,  # 开
            ContingentCondition=ApiStruct.CC_Immediately,  # 立即
            TimeCondition=ApiStruct.TC_GFD)  # 当日有效
        return rst

    async def sell(self, pos: Trade, limit_price: Decimal, volume: int):
        # 上期所区分平今和平昨
        close_flag = ApiStruct.OF_Close
        if pos.open_time.date() == datetime.datetime.today().replace(tzinfo=pytz.FixedOffset(480)).date() \
                and pos.instrument.exchange == ExchangeType.SHFE:
            close_flag = ApiStruct.OF_CloseToday
        rst = await self.ReqOrderInsert(
            InstrumentID=pos.code,
            VolumeTotalOriginal=volume,
            LimitPrice=float(limit_price),
            Direction=ApiStruct.D_Sell,  # 卖
            CombOffsetFlag=close_flag,  # 平
            ContingentCondition=ApiStruct.CC_Immediately,  # 立即
            TimeCondition=ApiStruct.TC_GFD)  # 当日有效
        return rst

    async def sell_short(self, inst: Instrument, limit_price: Decimal, volume: int):
        rst = await self.ReqOrderInsert(
            InstrumentID=inst.main_code,
            VolumeTotalOriginal=volume,
            LimitPrice=float(limit_price),
            Direction=ApiStruct.D_Sell,  # 卖
            CombOffsetFlag=ApiStruct.OF_Open,  # 开
            ContingentCondition=ApiStruct.CC_Immediately,  # 立即
            TimeCondition=ApiStruct.TC_GFD)  # 当日有效
        return rst

    async def buy_cover(self, pos: Trade, limit_price: Decimal, volume: int):
        # 上期所区分平今和平昨
        close_flag = ApiStruct.OF_Close
        if pos.open_time.date() == datetime.datetime.today().replace(tzinfo=pytz.FixedOffset(480)).date() \
                and pos.instrument.exchange == ExchangeType.SHFE:
            close_flag = ApiStruct.OF_CloseToday
        rst = await self.ReqOrderInsert(
            InstrumentID=pos.code,
            VolumeTotalOriginal=volume,
            LimitPrice=float(limit_price),
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

    # @param_function(channel='MSG:CTP:RSP:MARKET:OnRtnDepthMarketData:*')
    # async def OnRtnDepthMarketData(self, channel, tick: dict):
    #     """
    #     'PreOpenInterest': 50990,
    #     'TradingDay': '20160803',
    #     'SettlementPrice': 1.7976931348623157e+308,
    #     'AskVolume1': 40,
    #     'Volume': 11060,
    #     'LastPrice': 37740,
    #     'LowestPrice': 37720,
    #     'ClosePrice': 1.7976931348623157e+308,
    #     'ActionDay': '20160803',
    #     'UpdateMillisec': 0,
    #     'PreClosePrice': 37840,
    #     'LowerLimitPrice': 35490,
    #     'OpenInterest': 49460,
    #     'UpperLimitPrice': 40020,
    #     'AveragePrice': 189275.7233273056,
    #     'HighestPrice': 38230,
    #     'BidVolume1': 10,
    #     'UpdateTime': '11:03:12',
    #     'InstrumentID': 'cu1608',
    #     'PreSettlementPrice': 37760,
    #     'OpenPrice': 37990,
    #     'BidPrice1': 37740,
    #     'Turnover': 2093389500,
    #     'AskPrice1': 37750
    #     """
    #     try:
    #         inst = channel.split(':')[-1]
    #         tick['UpdateTime'] = datetime.datetime.strptime(tick['UpdateTime'], "%Y%m%d %H:%M:%S:%f")
    #         logger.info('inst=%s, tick: %s', inst, tick)
    #     except Exception as ee:
    #         logger.error('OnRtnDepthMarketData failed: %s', repr(ee), exc_info=True)

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

    @param_function(channel='MSG:CTP:RSP:TRADE:OnRtnInstrumentStatus:*')
    async def OnRtnInstrumentStatus(self, channel, status: dict):
        """
{"EnterReason":"1","EnterTime":"10:30:00","ExchangeID":"SHFE","ExchangeInstID":"ru","InstrumentID":"ru","InstrumentStatus":"2","SettlementGroupID":"00000001","TradingSegmentSN":27}
        """
        try:
            product_code = channel.split(':')[-1]
            inst = self.__strategy.instruments.filter(product_code=product_code).first()
            if inst is None or product_code not in self.__inst_ids:
                return
            if is_auction_time(inst, status):
                signal = Signal.objects.filter(strategy=self.__strategy, instrument=inst, processed=False).first()
                if signal == SignalType.BUY:
                    self.buy(inst, signal.price, signal.volume)
                elif signal == SignalType.SELL_SHORT:
                    self.sell_short(inst, signal.price, signal.volume)
                elif signal == SignalType.BUY_COVER:
                    pos = Trade.objects.filter(
                        close_time__isnull=True, exchange=inst.exchange, direction=DirectionType.SHORT,
                        instrument__product_code=product_code, shares__gt=0).first()
                    self.buy_cover(pos, signal.price, signal.volume)
                elif signal == SignalType.SELL:
                    pos = Trade.objects.filter(
                        close_time__isnull=True, exchange=inst.exchange, direction=DirectionType.LONG,
                        instrument__product_code=product_code, shares__gt=0).first()
                    self.sell(pos, signal.price, signal.volume)
                elif signal == SignalType.ROLLOVER:
                    pos = Trade.objects.filter(
                        close_time__isnull=True, exchange=inst.exchange,
                        instrument__product_code=product_code, shares__gt=0).first()
                    if pos.direction == DirectionType.LONG:
                        self.sell(pos, signal.trigger_value, signal.volume)
                        self.buy(inst, signal.price, signal.volume)
                    else:
                        self.buy_cover(pos, signal.trigger_value, signal.volume)
                        self.sell_short(inst, signal.price, signal.volume)
                if signal is not None:
                    signal.processed = True
                    signal.save(update_fields=['processed'])
        except Exception as ee:
            logger.error('OnRtnInstrumentStatus failed: %s', repr(ee), exc_info=True)

    @param_function(crontab='20 15 * * *')
    async def refresh_instrument(self):
        logger.info('更新账户')
        await self.query('TradingAccount')
        logger.info('更新持仓')
        pos_list = await self.query('InvestorPositionDetail')
        for pos in pos_list:
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
        self.update_position()
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
                    inst_dict[inst['ProductID']]['multiple'] = inst['VolumeMultiple']
                    inst_dict[inst['ProductID']]['price_tick'] = inst['PriceTick']
                    inst_dict[inst['ProductID']]['margin'] = inst['LongMarginRatio']
                    if 'name' not in inst_dict[inst['ProductID']]:
                        inst_dict[inst['ProductID']]['name'] = regex.match(inst['InstrumentName']).group(1)
        for code, data in inst_dict.items():
            logger.info('更新合约保证金: %s', code)
            inst, _ = Instrument.objects.update_or_create(product_code=code, defaults={
                'exchange': data['exchange'],
                'name': data['name'],
                'all_inst': ','.join(sorted(inst_set[code])),
                'volume_multiple': data['multiple'],
                'price_tick': data['price_tick'],
                'margin_rate': data['margin']
            })
            await self.update_inst_fee(inst)
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
            _, trading = await is_trading_day(day)
            if not trading:
                logger.info('今日是非交易日, 不计算任何数据。')
                return
            logger.info('每日盘后计算, day: %s, 获取交易所日线数据..', day)
            tasks = [
                self.io_loop.create_task(update_from_shfe(day)),
                self.io_loop.create_task(update_from_dce(day)),
                self.io_loop.create_task(update_from_czce(day)),
                self.io_loop.create_task(update_from_cffex(day)),
            ]
            await asyncio.wait(tasks)
            for inst_obj in Instrument.objects.all():
                logger.info('计算连续合约, 交易信号: %s', inst_obj.name)
                calc_main_inst(inst_obj, day)
                self.calc_signal(inst_obj, day)
        except Exception as e:
            logger.error('collect_quote failed: %s', e, exc_info=True)
        logger.info('盘后计算完毕!')

    @param_function(crontab='0 10 * * *')
    async def collect_tick_start(self):
        _, trading = await is_trading_day()
        if trading:
            await self.SubscribeMarketData(self.__inst_ids)

    @param_function(crontab='0 11 * * *')
    async def collect_tick_stop(self):
        _, trading = await is_trading_day()
        if trading:
            await self.UnSubscribeMarketData(self.__inst_ids)

    @param_function(crontab='30 15 * * *')
    async def update_equity(self):
        today, trading = await is_trading_day()
        if trading:
            dividend = Performance.objects.filter(
                broker=self.__broker, day__lt=today.date()).aggregate(Sum('dividend'))['dividend__sum']
            if dividend is None:
                dividend = Decimal(0)
            perform = Performance.objects.filter(
                broker=self.__broker, day__lt=today.date()).order_by('-day').first()
            if perform is None:
                unit = Decimal(1000000)
            else:
                unit = perform.unit_count
            nav = self.__current / unit
            accumulated = (self.__current - dividend) / (unit - dividend)
            Performance.objects.update_or_create(broker=self.__broker, day=today.date(), defaults={
                'used_margin': self.__margin,
                'capital': self.__current, 'unit_count': unit, 'NAV': nav, 'accumulated': accumulated})
    async def update_inst_fee(self, inst: Instrument):
        """
        更新每一个合约的手续费
        """
        try:
            fee = (await self.query('InstrumentCommissionRate', InstrumentID=inst.main_code))
            if fee:
                fee = fee[0]
                inst.fee_money = Decimal(fee['CloseRatioByMoney'])
                inst.fee_volume = Decimal(fee['CloseRatioByVolume'])
            all_insts = self.redis_client.keys(inst.product_code+'*')
            tar_code = inst.main_code
            for code in all_insts:
                if re.findall('[A-Za-z]+', code)[0] == inst.product_code:
                    tar_code = code
                    break
            tick = json.loads(self.redis_client.get(tar_code))
            inst.up_limit_ratio = (Decimal(tick['UpperLimitPrice']) -
                                   Decimal(tick['PreSettlementPrice'])) / Decimal(tick['PreSettlementPrice'])
            inst.up_limit_ratio = round(inst.up_limit_ratio, 3)
            inst.down_limit_ratio = (Decimal(tick['PreSettlementPrice']) -
                                     Decimal(tick['LowerLimitPrice'])) / Decimal(tick['PreSettlementPrice'])
            inst.down_limit_ratio = round(inst.down_limit_ratio, 3)
            inst.save(update_fields=['fee_money', 'fee_volume', 'margin_rate',
                                     'up_limit_ratio', 'down_limit_ratio'])
        except Exception as e:
            logger.error('update_inst_fee failed: %s', e, exc_info=True)

    def calc_signal(self, inst: Instrument,
                    day: datetime.datetime = datetime.datetime.today().replace(tzinfo=pytz.FixedOffset(480))):
        try:
            if inst.product_code not in self.__inst_ids:
                return
            break_n = self.__strategy.param_set.get(code='BreakPeriod').int_value
            atr_n = self.__strategy.param_set.get(code='AtrPeriod').int_value
            long_n = self.__strategy.param_set.get(code='LongPeriod').int_value
            short_n = self.__strategy.param_set.get(code='ShortPeriod').int_value
            stop_n = self.__strategy.param_set.get(code='StopLoss').int_value
            risk = self.__strategy.param_set.get(code='Risk').float_value
            last_bars = MainBar.objects.filter(
                exchange=inst.exchange, product_code=inst.product_code, time__lte=day
            ).order_by('time').values_list('open', 'high', 'low', 'close')
            arr = np.array(last_bars, dtype=float)
            close = arr[-1, 3]
            atr = talib.ATR(arr[:, 1], arr[:, 2], arr[:, 3], timeperiod=atr_n)[-1]
            short_trend = talib.SMA(arr[:, 3], timeperiod=short_n)[-1]
            long_trend = talib.SMA(arr[:, 3], timeperiod=long_n)[-1]
            high_line = np.amax(arr[-break_n:, 3])
            low_line = np.amin(arr[-break_n:, 3])
            buy_sig = short_trend > long_trend and close > high_line
            sell_sig = short_trend < long_trend and close < low_line
            # 查询该品种目前持有的仓位, 条件是开仓时间<=今天, 尚未未平仓或今天以后平仓(回测用)
            pos = Trade.objects.filter(
                Q(close_time__isnull=True) | Q(close_time__gt=day),
                instrument=inst, shares__gt=0, open_time__lt=day).first()
            roll_over = False
            if pos is not None:
                roll_over = pos.code != inst.main_code
            signal = None
            signal_value = None
            price = None
            volume = None
            if pos is not None:
                # 多头持仓
                if pos.direction == DirectionType.LONG:
                    hh = float(MainBar.objects.filter(
                        exchange=inst.exchange, product_code=pos.instrument.product_code,
                        time__gte=pos.open_time, time__lte=day).aggregate(Max('high'))['high__max'])
                    # 多头止损
                    if close <= hh - atr * stop_n:
                        signal = SignalType.SELL
                        # 止损时 signal_value 为止损价
                        signal_value = hh - atr * stop_n
                        volume = pos.shares
                        last_bar = DailyBar.objects.filter(
                            exchange=inst.exchange, code=pos.code, time=day.date()).first()
                        price = myround(last_bar.settlement * (Decimal(1)-inst.down_limit_ratio),
                                        inst.price_tick)
                    # 多头换月
                    elif roll_over:
                        signal = SignalType.ROLLOVER
                        volume = pos.shares
                        last_bar = DailyBar.objects.filter(
                            exchange=inst.exchange, code=pos.code, time=day.date()).first()
                        # 换月时 signal_value 为旧合约的平仓价
                        signal_value = myround(last_bar.settlement * (Decimal(1) - inst.down_limit_ratio),
                                               inst.price_tick)
                        new_bar = DailyBar.objects.filter(
                            exchange=inst.exchange, code=inst.main_code, time=day.date()).first()
                        price = myround(new_bar.settlement * (Decimal(1) + inst.up_limit_ratio),
                                        inst.price_tick)

                # 空头持仓
                else:
                    ll = float(MainBar.objects.filter(
                        exchange=inst.exchange, product_code=pos.instrument.product_code,
                        time__gte=pos.open_time, time__lte=day).aggregate(Min('low'))['low__min'])
                    # 空头止损
                    if close >= ll + atr * stop_n:
                        signal = SignalType.BUY_COVER
                        signal_value = ll + atr * stop_n
                        volume = pos.shares
                        last_bar = DailyBar.objects.filter(
                            exchange=inst.exchange, code=pos.code, time=day.date()).first()
                        price = myround(last_bar.settlement * (Decimal(1) + inst.down_limit_ratio),
                                        inst.price_tick)
                    # 空头换月
                    elif roll_over:
                        signal = SignalType.ROLLOVER
                        volume = pos.shares
                        last_bar = DailyBar.objects.filter(
                            exchange=inst.exchange, code=pos.code, time=day.date()).first()
                        signal_value = myround(last_bar.settlement * (Decimal(1) + inst.up_limit_ratio),
                                               inst.price_tick)
                        new_bar = DailyBar.objects.filter(
                            exchange=inst.exchange, code=inst.main_code, time=day.date()).first()
                        price = myround(new_bar.settlement * (Decimal(1) - inst.down_limit_ratio),
                                        inst.price_tick)
            # 做多
            elif buy_sig:
                signal = SignalType.BUY
                volume = self.__current * risk // (Decimal(atr) * Decimal(inst.volume_multiple))
                signal_value = high_line
                new_bar = DailyBar.objects.filter(
                    exchange=inst.exchange, code=inst.main_code, time=day.date()).first()
                price = myround(new_bar.settlement * (Decimal(1) + inst.up_limit_ratio),
                                inst.price_tick)
            # 做空
            elif sell_sig:
                signal = SignalType.SELL_SHORT
                volume = self.__current * risk // (Decimal(atr) * Decimal(inst.volume_multiple))
                signal_value = low_line
                new_bar = DailyBar.objects.filter(
                    exchange=inst.exchange, code=inst.main_code, time=day.date()).first()
                price = myround(new_bar.settlement * (Decimal(1) - inst.down_limit_ratio),
                                inst.price_tick)
            if signal is not None:
                Signal.objects.update_or_create(
                    strategy=self.__strategy, instrument=inst, type=signal, trigger_time=day, defaults={
                        'price': price, 'volume': volume, 'trigger_value': signal_value,
                        'priority': PriorityType.Normal, 'processed': False})
        except Exception as e:
            logger.error('calc_signal failed: %s', e, exc_info=True)

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
