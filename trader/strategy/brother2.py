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

    def update_order(self, order: dict):
        if order['OrderStatus'] == ApiStruct.OST_NotTouched and \
                        order['OrderSubmitStatus'] == ApiStruct.OSS_InsertSubmitted:
            if self.__activeOrders.get(order['OrderRef']) is None:
                #     logger.info(u"OpenOrder=%s", order)
                #     build_order = self.build_order_from_open_order(order, self.getInstrumentTraits())
                #     self._register_order(build_order)
                #     self.update_order(order)
                pass

    def update_account(self, account: dict):
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
                exchange=pos['ExchangeID'],
                product_code=re.findall('[A-Za-z]+', pos['InstrumentID'])[0]).first()
            Trade.objects.update_or_create(
                broker=self.__broker, strategy=self.__strategy, instrument=inst,
                code=pos['InstrumentID'],
                direction=DirectionType.LONG if pos['Direction'] == ApiStruct.D_Buy else DirectionType.SHORT,
                close_time__isnull=True,
                defaults={
                    'open_time': datetime.datetime.strptime(
                        pos['OpenDate']+'09', '%Y%m%d%H').replace(tzinfo=pytz.FixedOffset(480)),
                    'shares': pos['Volume'], 'filled_shares': pos['Volume'],
                    'avg_entry_price': Decimal(pos['OpenPrice']),
                    'cost': pos['Volume'] * Decimal(pos['OpenPrice']) * inst.fee_money *
                            inst.volume_multiple + pos['Volume'] * inst.fee_volume,
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
            if ('bIsLast' not in msg_dict or msg_dict['bIsLast']) and not cb.done():
                cb.set_result(msg_list)

    async def start(self):
        await self.query('TradingAccount')
        await self.query('InvestorPositionDetail')
        # await self.collect_tick_stop()
        # await self.collect_quote()
        # day = datetime.datetime.strptime('20160905', '%Y%m%d').replace(tzinfo=pytz.FixedOffset(480))
        # for inst in self.__strategy.instruments.all():
        #   self.calc_signal(inst, day)
        #   self.process_signal(inst)
        # order_list = await self.query('Order')
        # if order_list:
        #     for order in order_list:
        #         if order['OrderStatus'] == ApiStruct.OST_NotTouched and \
        #                         order['OrderSubmitStatus'] == ApiStruct.OSS_InsertSubmitted:
        #             self.__activeOrders[order['OrderRef']] = order
        #     logger.info("未成交订单: %s", self.__activeOrders)
        # await self.SubscribeMarketData(self.__inst_ids)

    async def stop(self):
        pass
        # if self.__inst_ids:
        #     await self.UnSubscribeMarketData(self.__inst_ids)

    def next_order_ref(self):
        self.__order_ref = 1 if self.__order_ref == 999 else self.__order_ref + 1
        now = datetime.datetime.now()
        return '{:02}{:02}{:02}{:03}{:03}'.format(
            now.hour, now.minute, now.second, int(now.microsecond / 1000), self.__order_ref)

    def next_id(self):
        self.__request_id = 1 if self.__request_id == 65535 else self.__request_id + 1
        return self.__request_id

    def getShares(self, instrument: str):
        # 这个函数只能处理持有单一方向仓位的情况，若同时持有多空的头寸，返回结果不正确
        shares = 0
        pos_price = 0
        for pos in self.__shares[instrument]:
            pos_price += pos['Volume'] * pos['OpenPrice']
            shares += pos['Volume'] * (-1 if pos['Direction'] == ApiStruct.D_Sell else 1)
        return shares, pos_price / abs(shares), self.__shares[instrument][0]['OpenDate']

    def getPositions(self, inst_id: int):
        # 这个函数只能处理持有单一方向仓位的情况，若同时持有多空的头寸，返回结果不正确
        return self.__shares[inst_id][0]

    def async_query(self, query_type: str, **kwargs):
        request_id = self.next_id()
        kwargs['RequestID'] = request_id
        self.redis_client.publish(self.__request_format.format('ReqQry' + query_type), json.dumps(kwargs))

    async def query(self, query_type: str, **kwargs):
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
            channel_name1 = self.__trade_response_format.format('OnRtnOrder', order_ref)
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
    #         if datetime.datetime.now().hour < 9:
    #             logger.info('inst=%s, tick: %s', inst, tick)
    #     except Exception as ee:
    #         logger.error('OnRtnDepthMarketData failed: %s', repr(ee), exc_info=True)

    @param_function(channel='MSG:CTP:RSP:TRADE:OnRtnTrade:*')
    async def OnRtnTrade(self, channel, trade: dict):
        try:
            signal = None
            order_ref = channel.split(':')[-1]
            # logger.info('order_ref=%s, trade: %s', order_ref, trade)
            inst = Instrument.objects.get(product_code=re.findall('[A-Za-z]+', trade['InstrumentID'])[0])
            order = Order.objects.filter(order_ref=order_ref).first()
            if trade['OffsetFlag'] == ApiStruct.OF_Open:
                last_trade, created = Trade.objects.update_or_create(
                    broker=self.__broker, strategy=self.__strategy, instrument=inst,
                    code=trade['InstrumentID'],
                    open_time__date=datetime.datetime.strptime(trade['TradingDay'], '%Y%m%d').replace(
                        tzinfo=pytz.FixedOffset(480)).date(),
                    direction=DirectionType.LONG if trade['Direction'] == ApiStruct.D_Buy
                    else DirectionType.SHORT, close_time__isnull=True, defaults={
                        'open_order': order,
                        'open_time': datetime.datetime.strptime(
                            trade['TradeDate']+trade['TradeTime'], '%Y%m%d%H:%M:%S').replace(
                            tzinfo=pytz.FixedOffset(480))})
                if created:
                    last_trade.filled_shares = trade['Volume']
                    last_trade.shares = order.volume if order is not None else trade['Volume']
                    last_trade.avg_entry_price = trade['Price']
                    last_trade.cost = \
                        trade['Volume'] * Decimal(trade['Price']) * inst.fee_money * \
                        inst.volume_multiple + trade['Volume'] * inst.fee_volume
                    last_trade.frozen_margin = trade['Volume'] * Decimal(trade['Price']) * inst.margin_rate
                else:
                    if last_trade is not None:
                        if last_trade.filled_shares is None:
                            last_trade.filled_shares = 0
                        if last_trade.avg_entry_price is None:
                            last_trade.avg_entry_price = Decimal(0)
                    last_trade.avg_entry_price = \
                        (last_trade.avg_entry_price * last_trade.filled_shares + trade['Volume'] *
                         trade['Price']) / (last_trade.filled_shares + trade['Volume'])
                    last_trade.filled_shares += trade['Volume']
                    last_trade.cost += \
                        trade['Volume'] * Decimal(trade['Price']) * inst.fee_money * \
                        inst.volume_multiple + trade['Volume'] * inst.fee_volume
                    last_trade.frozen_margin += trade['Volume'] * Decimal(trade['Price']) * inst.margin_rate
                last_trade.save()
                signal = Signal.objects.filter(
                    Q(type=SignalType.BUY if trade['Direction'] == ApiStruct.D_Buy
                        else SignalType.SELL_SHORT) | Q(type=SignalType.ROLL_OPEN),
                    code=trade['InstrumentID'], volume=last_trade.filled_shares,
                    strategy=self.__strategy, instrument=inst, processed=False)
            else:
                last_trade = Trade.objects.filter(
                    broker=self.__broker, strategy=self.__strategy, instrument=inst,
                    code=trade['InstrumentID'],
                    direction=DirectionType.LONG if trade['Direction'] == ApiStruct.D_Sell
                    else DirectionType.SHORT, close_time__isnull=True).first()
                if last_trade is not None:
                    if last_trade.closed_shares is None:
                        last_trade.closed_shares = 0
                    if last_trade.avg_exit_price is None:
                        last_trade.avg_exit_price = Decimal(0)
                    last_trade.avg_exit_price = \
                        (last_trade.avg_exit_price * last_trade.closed_shares +
                         trade['Volume'] * trade['Price']) / (last_trade.closed_shares + trade['Volume'])
                    last_trade.closed_shares += trade['Volume']
                    last_trade.cost += \
                        trade['Volume'] * Decimal(trade['Price']) * inst.fee_money * \
                        inst.volume_multiple + trade['Volume'] * inst.fee_volume
                    if last_trade.closed_shares == last_trade.shares:
                        # 全部成交
                        last_trade.close_order = order
                        last_trade.close_time = datetime.datetime.strptime(
                            trade['TradeDate']+trade['TradeTime'], '%Y%m%d%H:%M:%S').replace(
                            tzinfo=pytz.FixedOffset(480))
                    last_trade.save()
                    signal = Signal.objects.filter(
                        Q(type=SignalType.BUY_COVER if trade['Direction'] == ApiStruct.D_Buy
                            else SignalType.SELL) | Q(type=SignalType.ROLL_CLOSE),
                        code=trade['InstrumentID'], volume=last_trade.closed_shares,
                        strategy=self.__strategy, instrument=inst, processed=False)
            if signal is not None:
                signal.update(processed=True)
        except Exception as ee:
            logger.error('OnRtnTrade failed: %s', repr(ee), exc_info=True)

    @param_function(channel='MSG:CTP:RSP:TRADE:OnRtnOrder:*')
    async def OnRtnOrder(self, channel, order: dict):
        try:
            order_ref = channel.split(':')[-1]
            # logger.info('order_ref=%s, order: %s', order_ref, order)
            inst = Instrument.objects.get(product_code=re.findall('[A-Za-z]+', order['InstrumentID'])[0])
            Order.objects.update_or_create(order_ref=order_ref, defaults={
                'broker': self.__broker, 'strategy': self.__strategy, 'instrument': inst,
                'code': order['InstrumentID'], 'front': order['FrontID'], 'session': order['SessionID'],
                'price': order['LimitPrice'], 'volume': order['VolumeTotalOriginal'],
                'direction': DirectionType.LONG if order['Direction'] == ApiStruct.D_Buy else DirectionType.SHORT,
                'offset_flag': OffsetFlag.OPEN if order['CombOffsetFlag'] == ApiStruct.OF_Open else OffsetFlag.CLOSE,
                'status': order['OrderStatus'],
                'send_time': datetime.datetime.strptime(
                    order['InsertDate']+order['InsertTime'], '%Y%m%d%H:%M:%S').replace(
                    tzinfo=pytz.FixedOffset(480)),
                'update_time': datetime.datetime.now().replace(tzinfo=pytz.FixedOffset(480))
            })
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
            # logger.info('合约状态通知: %s %s', inst, status)
            if is_auction_time(inst, status):
                logger.info('%s 开始集合竞价, 查询待处理信号..', inst)
                self.process_signal(inst)
        except Exception as ee:
            logger.error('OnRtnInstrumentStatus failed: %s', repr(ee), exc_info=True)

    @param_function(crontab='1 9 * * *')
    async def check_signal_processed1(self):
        day = datetime.datetime.today()
        day = day.replace(tzinfo=pytz.FixedOffset(480))
        _, trading = await is_trading_day(day)
        if trading:
            logger.info('查询遗漏的日盘信号..')
            for sig in Signal.objects.filter(
                    ~Q(instrument__exchange=ExchangeType.CFFEX),
                    strategy=self.__strategy, instrument__night_trade=False, processed=False).all():
                logger.info('发现遗漏信号: %s', sig)
                self.process_signal(sig.instrument, use_tick=True)

    @param_function(crontab='16 9 * * *')
    async def check_signal_processed2(self):
        day = datetime.datetime.today()
        day = day.replace(tzinfo=pytz.FixedOffset(480))
        _, trading = await is_trading_day(day)
        if trading:
            logger.info('查询遗漏的国债信号..')
            for sig in Signal.objects.filter(
                    instrument__exchange=ExchangeType.CFFEX,
                    strategy=self.__strategy, instrument__night_trade=False, processed=False).all():
                logger.info('发现遗漏信号: %s', sig)
                self.process_signal(sig.instrument, use_tick=True)

    @param_function(crontab='1 21 * * *')
    async def check_signal_processed3(self):
        day = datetime.datetime.today()
        day = day.replace(tzinfo=pytz.FixedOffset(480))
        _, trading = await is_trading_day(day)
        if trading:
            logger.info('查询遗漏的夜盘信号..')
            for sig in Signal.objects.filter(
                    ~Q(instrument__exchange=ExchangeType.CFFEX),
                    strategy=self.__strategy, instrument__night_trade=True, processed=False).all():
                logger.info('发现遗漏信号: %s', sig)
                self.process_signal(sig.instrument, use_tick=True)

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

    @param_function(crontab='57 8 * * *')
    async def collect_day_tick_start(self):
        day = datetime.datetime.today().replace(tzinfo=pytz.FixedOffset(480))
        day, trading = await is_trading_day(day)
        if trading:
            logger.info('订阅全品种行情, %s %s', day, trading)
            inst_set = list()
            for inst in Instrument.objects.all():
                inst_set += inst.all_inst.split(',')
            await self.SubscribeMarketData(inst_set)

    @param_function(crontab='0 10 * * *')
    async def collect_day_tick_stop(self):
        day = datetime.datetime.today().replace(tzinfo=pytz.FixedOffset(480))
        day, trading = await is_trading_day(day)
        if trading:
            logger.info('取消订阅全品种行情, %s %s', day, trading)
            inst_set = list()
            for inst in Instrument.objects.all():
                inst_set += inst.all_inst.split(',')
            await self.UnSubscribeMarketData(inst_set)

    @param_function(crontab='57 20 * * *')
    async def collect_night_tick_start(self):
        day = datetime.datetime.today().replace(tzinfo=pytz.FixedOffset(480))
        day, trading = await is_trading_day(day)
        if trading:
            logger.info('订阅全品种行情, %s %s', day, trading)
            inst_set = list()
            for inst in Instrument.objects.all():
                inst_set += inst.all_inst.split(',')
            await self.SubscribeMarketData(inst_set)

    @param_function(crontab='0 22 * * *')
    async def collect_night_tick_stop(self):
        day = datetime.datetime.today().replace(tzinfo=pytz.FixedOffset(480))
        day, trading = await is_trading_day(day)
        if trading:
            logger.info('取消订阅全品种行情, %s %s', day, trading)
            inst_set = list()
            for inst in Instrument.objects.all():
                inst_set += inst.all_inst.split(',')
            await self.UnSubscribeMarketData(inst_set)

    @param_function(crontab='30 15 * * *')
    async def update_equity(self):
        today, trading = await is_trading_day(datetime.datetime.today().replace(tzinfo=pytz.FixedOffset(480)))
        if trading:
            logger.info('更新资金净值 %s %s', today, trading)
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
            tick = json.loads(self.redis_client.get(inst.main_code))
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

    def calc_signal(self, inst: Instrument, day: datetime.datetime):
        try:
            if inst.product_code not in self.__inst_ids:
                return
            break_n = self.__strategy.param_set.get(code='BreakPeriod').int_value + 1
            atr_n = self.__strategy.param_set.get(code='AtrPeriod').int_value
            long_n = self.__strategy.param_set.get(code='LongPeriod').int_value
            short_n = self.__strategy.param_set.get(code='ShortPeriod').int_value
            stop_n = self.__strategy.param_set.get(code='StopLoss').int_value
            risk = self.__strategy.param_set.get(code='Risk').float_value
            last_bars = MainBar.objects.filter(
                exchange=inst.exchange, product_code=inst.product_code, time__lte=day.date()
            ).order_by('time').values_list('open', 'high', 'low', 'close')
            arr = np.array(last_bars, dtype=float)
            close = arr[-1, 3]
            atr_s = talib.ATR(arr[:, 1], arr[:, 2], arr[:, 3], timeperiod=atr_n)
            atr = atr_s[-1]
            short_trend = talib.SMA(arr[:, 3], timeperiod=short_n)[-1]
            long_trend = talib.SMA(arr[:, 3], timeperiod=long_n)[-1]
            high_line = np.amax(arr[-break_n:-1, 3])
            low_line = np.amin(arr[-break_n:-1, 3])
            buy_sig = short_trend > long_trend and close > high_line
            sell_sig = short_trend < long_trend and close < low_line
            # 查询该品种目前持有的仓位, 条件是开仓时间<=今天, 尚未平仓或今天以后平仓(回测用)
            pos = Trade.objects.filter(
                Q(close_time__isnull=True) | Q(close_time__gt=day),
                instrument=inst, shares__gt=0, open_time__lt=day).first()
            roll_over = False
            open_count = 1
            if pos is not None:
                roll_over = pos.code != inst.main_code
                open_count = MainBar.objects.filter(
                    exchange=inst.exchange, product_code=inst.product_code,
                    time__gte=pos.open_time.date(), time__lte=day.date()).count()
            elif self.__strategy.force_opens.filter(id=inst.id).exists() and not buy_sig and not sell_sig:
                    logger.info('强制开仓: %s', inst)
                    if short_trend > long_trend:
                        buy_sig = True
                    else:
                        sell_sig = True
                    self.__strategy.force_opens.remove(inst)
            signal = None
            signal_value = None
            price = None
            volume = None
            if pos is not None:
                # 多头持仓
                if pos.direction == DirectionType.LONG:
                    hh = float(MainBar.objects.filter(
                        exchange=inst.exchange, product_code=pos.instrument.product_code,
                        time__gte=pos.open_time.date(), time__lte=day).aggregate(Max('high'))['high__max'])
                    # 多头止损
                    if close <= hh - atr_s[-open_count] * stop_n:
                        signal = SignalType.SELL
                        # 止损时 signal_value 为止损价
                        signal_value = hh - atr_s[-open_count] * stop_n
                        volume = pos.shares
                        last_bar = DailyBar.objects.filter(
                            exchange=inst.exchange, code=pos.code, time=day.date()).first()
                        price = self.calc_down_limit(inst, last_bar)
                    # 多头换月
                    elif roll_over:
                        signal = SignalType.ROLL_OPEN
                        volume = pos.shares
                        last_bar = DailyBar.objects.filter(
                            exchange=inst.exchange, code=pos.code, time=day.date()).first()
                        # 换月时 signal_value 为旧合约的平仓价
                        signal_value = self.calc_down_limit(inst, last_bar)
                        new_bar = DailyBar.objects.filter(
                            exchange=inst.exchange, code=inst.main_code, time=day.date()).first()
                        price = self.calc_up_limit(inst, new_bar)
                        Signal.objects.update_or_create(
                            code=pos.code, strategy=self.__strategy, instrument=inst,
                            type=SignalType.ROLL_CLOSE, trigger_time=day, defaults={
                                'price': signal_value, 'volume': volume,
                                'priority': PriorityType.Normal, 'processed': False})
                # 空头持仓
                else:
                    ll = float(MainBar.objects.filter(
                        exchange=inst.exchange, product_code=pos.instrument.product_code,
                        time__gte=pos.open_time.date(), time__lte=day).aggregate(Min('low'))['low__min'])
                    # 空头止损
                    if close >= ll + atr_s[-open_count] * stop_n:
                        signal = SignalType.BUY_COVER
                        signal_value = ll + atr_s[-open_count] * stop_n
                        volume = pos.shares
                        last_bar = DailyBar.objects.filter(
                            exchange=inst.exchange, code=pos.code, time=day.date()).first()
                        price = self.calc_up_limit(inst, last_bar)
                    # 空头换月
                    elif roll_over:
                        signal = SignalType.ROLL_OPEN
                        volume = pos.shares
                        last_bar = DailyBar.objects.filter(
                            exchange=inst.exchange, code=pos.code, time=day.date()).first()
                        signal_value = self.calc_up_limit(inst, last_bar)
                        new_bar = DailyBar.objects.filter(
                            exchange=inst.exchange, code=inst.main_code, time=day.date()).first()
                        price = self.calc_down_limit(inst, new_bar)
                        Signal.objects.update_or_create(
                            code=pos.code, strategy=self.__strategy, instrument=inst,
                            type=SignalType.ROLL_CLOSE, trigger_time=day, defaults={
                                'price': signal_value, 'volume': volume,
                                'priority': PriorityType.Normal, 'processed': False})
            # 做多
            elif buy_sig:
                volume = self.__current * risk // (Decimal(atr) * Decimal(inst.volume_multiple))
                if volume > 0:
                    signal = SignalType.BUY
                    signal_value = high_line
                    new_bar = DailyBar.objects.filter(
                        exchange=inst.exchange, code=inst.main_code, time=day.date()).first()
                    price = self.calc_up_limit(inst, new_bar)
            # 做空
            elif sell_sig:
                volume = self.__current * risk // (Decimal(atr) * Decimal(inst.volume_multiple))
                if volume > 0:
                    signal = SignalType.SELL_SHORT
                    signal_value = low_line
                    new_bar = DailyBar.objects.filter(
                        exchange=inst.exchange, code=inst.main_code, time=day.date()).first()
                    price = self.calc_down_limit(inst, new_bar)
            if signal is not None:
                Signal.objects.update_or_create(
                    code=inst.main_code,
                    strategy=self.__strategy, instrument=inst, type=signal, trigger_time=day, defaults={
                        'price': price, 'volume': volume, 'trigger_value': signal_value,
                        'priority': PriorityType.Normal, 'processed': False})
        except Exception as e:
            logger.error('calc_signal failed: %s', e, exc_info=True)

    def process_signal(self, inst: Instrument, use_tick: bool=False):
        """
        :param inst: 合约
        :param use_tick: 是否使用当天tick的涨跌停价下单
        :return: None
        """
        signals = Signal.objects.filter(strategy=self.__strategy, instrument=inst, processed=False).all()
        logger.info('%s 当前信号: %s', inst, signals)
        if not signals:
            return
        for signal in signals:
            price = signal.price
            if signal.type == SignalType.BUY:
                if use_tick:
                    tick = json.loads(self.redis_client.get(signal.code))
                    price = tick['UpperLimitPrice']
                logger.info('%s 开多%s手 价格: %s', inst, signal.volume, price)
                self.io_loop.create_task(self.buy(inst, price, signal.volume))
            elif signal.type == SignalType.SELL_SHORT:
                if use_tick:
                    tick = json.loads(self.redis_client.get(signal.code))
                    price = tick['LowerLimitPrice']
                logger.info('%s 开空%s手 价格: %s', inst, signal.volume, price)
                self.io_loop.create_task(self.sell_short(inst, price, signal.volume))
            elif signal.type == SignalType.BUY_COVER:
                pos = Trade.objects.filter(
                    code=signal.code, close_time__isnull=True, direction=DirectionType.SHORT,
                    instrument=inst, shares__gt=0).first()
                if use_tick:
                    tick = json.loads(self.redis_client.get(signal.code))
                    price = tick['UpperLimitPrice']
                logger.info('%s 平空%s手 价格: %s', pos.instrument, signal.volume, price)
                self.io_loop.create_task(self.buy_cover(pos, price, signal.volume))
            elif signal.type == SignalType.SELL:
                pos = Trade.objects.filter(
                    code=signal.code, close_time__isnull=True, direction=DirectionType.LONG,
                    instrument=inst, shares__gt=0).first()
                if use_tick:
                    tick = json.loads(self.redis_client.get(signal.code))
                    price = tick['LowerLimitPrice']
                logger.info('%s 平多%s手 价格: %s', pos.instrument, signal.volume, price)
                self.io_loop.create_task(self.sell(pos, price, signal.volume))
            elif signal.type == SignalType.ROLL_CLOSE:
                pos = Trade.objects.filter(
                    code=signal.code, close_time__isnull=True, instrument=inst, shares__gt=0).first()
                if pos.direction == DirectionType.LONG:
                    if use_tick:
                        tick = json.loads(self.redis_client.get(signal.code))
                        price = tick['LowerLimitPrice']
                    logger.info('%s->%s 多头换月平旧%s手 价格: %s', pos.code, inst.main_code, signal.volume, price)
                    self.io_loop.create_task(self.sell(pos, price, signal.volume))
                else:
                    logger.info('%s->%s 空头换月平旧%s手 价格: %s', pos.code, inst.main_code, signal.volume, price)
                    if use_tick:
                        tick = json.loads(self.redis_client.get(signal.code))
                        price = tick['UpperLimitPrice']
                    self.io_loop.create_task(self.buy_cover(pos, price, signal.volume))
            elif signal.type == SignalType.ROLL_OPEN:
                pos = Trade.objects.filter(
                    Q(close_time__isnull=True) | Q(close_time__date=datetime.datetime.today().date()),
                    shares=signal.volume, code=inst.last_main, instrument=inst, shares__gt=0).first()
                if pos.direction == DirectionType.LONG:
                    if use_tick:
                        tick = json.loads(self.redis_client.get(signal.code))
                        price = tick['UpperLimitPrice']
                    logger.info('%s->%s 多头换月开新%s手 价格: %s', pos.code, inst.main_code, signal.volume, price)
                    self.io_loop.create_task(self.buy(inst, price, signal.volume))
                else:
                    if use_tick:
                        tick = json.loads(self.redis_client.get(signal.code))
                        price = tick['LowerLimitPrice']
                    logger.info('%s->%s 空头换月开新%s手 价格: %s', pos.code, inst.main_code, signal.volume, price)
                    self.io_loop.create_task(self.sell_short(inst, price, signal.volume))

    def calc_up_limit(self, inst: Instrument, bar: DailyBar):
        tick = json.loads(self.redis_client.get(bar.code))
        ratio = (Decimal(tick['UpperLimitPrice']) -
                 Decimal(tick['PreSettlementPrice'])) / Decimal(tick['PreSettlementPrice'])
        ratio = Decimal(round(ratio, 3))
        price = myround(bar.settlement * (Decimal(1) + ratio), inst.price_tick)
        return price - inst.price_tick

    def calc_down_limit(self, inst: Instrument, bar: DailyBar):
        tick = json.loads(self.redis_client.get(bar.code))
        ratio = (Decimal(tick['PreSettlementPrice']) -
                 Decimal(tick['LowerLimitPrice'])) / Decimal(tick['PreSettlementPrice'])
        ratio = Decimal(round(ratio, 3))
        price = myround(bar.settlement * (Decimal(1) - ratio), inst.price_tick)
        return price + inst.price_tick
