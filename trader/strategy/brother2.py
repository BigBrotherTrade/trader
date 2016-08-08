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
import random
import re
import ujson as json
from collections import defaultdict
import datetime

import aioredis

from trader.utils import logger as my_logger
from trader.strategy import BaseModule
from trader.utils.func_container import param_function
from trader.utils.read_config import *
from trader.utils import ApiStruct

logger = my_logger.get_logger('CTPApi')
HANDLER_TIME_OUT = config.getint('TRADE', 'command_timeout', fallback=10)


class TradeStrategy(BaseModule):
    __market_response_format = config.get('MSG_CHANNEL', 'market_response_format')
    __trade_response_format = config.get('MSG_CHANNEL', 'trade_response_format')
    __request_format = config.get('MSG_CHANNEL', 'request_format')
    __request_id = random.randint(0, 65535)
    __order_ref = random.randint(0, 999)
    __inst_ids = ['IF1609']
    __instruments = defaultdict(dict)
    __current = 0  # 当前动态权益
    __pre_balance = 0  # 静态权益
    __cash = 0  # 可用资金
    __shares = defaultdict(list)  # { instrument : [ posLong, posShort ] }
    __cur_account = None
    __activeOrders = {}  # 未成交委托单
    __waiting_orders = {}
    __cancel_orders = {}
    __initialed = False
    __login_time = None
    # 提合约字母部分 IF1509 -> IF
    __re_extract = re.compile(r'([a-zA-Z]*)(\d+)')
    # { inst_id : { 'info' : Instrument, 'fee' : InstrumentCommissionRate, 'margin' : InstrumentMarginRate } }
    __last_time = None
    __watch_pos = {}
    __ATR = {}

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
        logger.info(u"可用资金: %s,静态权益: %s,动态权益: %s", self.__cash, self.__pre_balance, self.__current)

    def calc_fee(self, trade: dict):
        inst = trade['InstrumentID']
        if inst not in self.__instruments:
            return -1
        action = trade['OffsetFlag']
        price = trade['Price']
        volume = trade['Volume']
        if action == ApiStruct.OF_Open:
            return self.__instruments[inst]['info']['VolumeMultiple'] * price * volume * self.__instruments[inst]['fee']['OpenRatioByMoney'] + volume * self.__instruments[inst]['fee']['OpenRatioByVolume']
        elif action == ApiStruct.OF_Close:
            return self.__instruments[inst]['info']['VolumeMultiple'] * price * volume * self.__instruments[inst]['fee']['CloseRatioByMoney'] + volume * self.__instruments[inst]['fee']['CloseRatioByVolume']
        elif action == ApiStruct.OF_CloseToday:
            return self.__instruments[inst]['info']['VolumeMultiple'] * price * volume * self.__instruments[inst]['fee']['CloseTodayRatioByMoney'] + volume * self.__instruments[inst]['fee']['CloseTodayRatioByVolume']
        else:
            return 0

    @staticmethod
    async def query_reader(ch: aioredis.Channel, cb: asyncio.Future):
        msg_list = []
        while await ch.wait_message():
            _, msg = await ch.get(encoding='utf-8')
            msg_dict = json.loads(msg)
            msg_list.append(msg_dict)
            if msg_dict['bIsLast'] and not cb.done():
                cb.set_result(msg_list)

    async def start(self):
        inst_list = await self.query('Instrument')
        for inst in inst_list:
            if inst['IsTrading'] == 0:
                continue
            self.__shares[inst['InstrumentID']].append(inst)
            self.__instruments[inst['InstrumentID']]['info'] = inst
            inst_fee = await self.query('InstrumentCommissionRate', InstrumentID=inst['InstrumentID'])
            self.__instruments[inst.InstrumentID]['fee'] = inst_fee[0]
            inst_margin = await self.query('InstrumentMarginRate', InstrumentID=inst['InstrumentID'])
            self.__instruments[inst.InstrumentID]['margin'] = inst_margin[0]

        account = await self.query('TradingAccount')
        self.update_account(account)

        pos_list = await self.query('InvestorPositionDetail')
        if pos_list:
            for pos in pos_list:
                if pos['Volume'] > 0:
                    self.__shares[pos['InstrumentID']].append(pos)

        order_list = await self.query('Order')
        if order_list:
            for order in order_list:
                if order['OrderStatus'] == ApiStruct.OST_NotTouched and order['OrderSubmitStatus'] == ApiStruct.OSS_InsertSubmitted:
                    self.__activeOrders[order['OrderRef']] = order
            logger.info("未成交订单: %s", self.__activeOrders)

        await self.SubscribeMarketData(self.__inst_ids)

    async def stop(self):
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
        for pos in self.__shares[instrument]:
            shares += pos['Volume'] * (-1 if pos.Direction == ApiStruct.D_Sell else 1)
        return shares

    def getPositions(self, inst_id):
        # 这个函数只能处理持有单一方向仓位的情况，若同时持有多空的头寸，返回结果不正确
        return self.__shares[inst_id][0]

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

    async def ReqOrderAction(self, inst_ids: list):
        pass

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
            logger.info('inst=%s, tick: %s', inst, tick)
        except Exception as ee:
            logger.error('OnRtnDepthMarketData failed: %s', repr(ee), exc_info=True)

    @param_function(channel='MSG:CTP:RSP:TRADE::OnRtnTrade:*')
    async def OnRtnTrade(self, channel, trade: dict):
        try:
            order_ref = channel.split(':')[-1]
            logger.info('order_ref=%s, trade: %s', order_ref, trade)
        except Exception as ee:
            logger.error('OnRtnTrade failed: %s', repr(ee), exc_info=True)

    @param_function(channel='MSG:CTP:RSP:TRADE::OnRtnOrder:*')
    async def OnRtnOrder(self, channel, order: dict):
        try:
            order_ref = channel.split(':')[-1]
            logger.info('order_ref=%s, order: %s', order_ref, order)
        except Exception as ee:
            logger.error('OnRtnOrder failed: %s', repr(ee), exc_info=True)

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
    #