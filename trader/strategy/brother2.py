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
import logging
from django.db.models import Q, F, Max, Min, Sum
from django.utils import timezone
from django.db import connection
from talib import ATR
import ujson as json
import aioredis

from trader.strategy import BaseModule
from trader.utils.func_container import RegisterCallback
from trader.utils.read_config import config, ctp_errors
from trader.utils import ApiStruct, price_round, is_trading_day, update_from_shfe, update_from_dce, \
    update_from_czce, update_from_cffex, get_contracts_argument, calc_main_inst, str_to_number
from panel.models import *

logger = logging.getLogger('CTPApi')
HANDLER_TIME_OUT = config.getint('TRADE', 'command_timeout', fallback=10)


class TradeStrategy(BaseModule):
    def __init__(self, name: str):
        super().__init__()
        self.__market_response_format = config.get('MSG_CHANNEL', 'market_response_format')
        self.__trade_response_format = config.get('MSG_CHANNEL', 'trade_response_format')
        self.__request_format = config.get('MSG_CHANNEL', 'request_format')
        self.__ignore_inst_list = config.get('TRADE', 'ignore_inst', fallback="WH,bb,JR,RI,RS,LR,PM,im").split(',')
        self.__request_id = random.randint(0, 65535)
        self.__order_ref = random.randint(0, 999)
        self.__strategy = Strategy.objects.get(name=name)
        self.__inst_ids = self.__strategy.instruments.all().values_list('product_code', flat=True)
        self.__broker = self.__strategy.broker
        self.__fake = self.__broker.fake  # 虚拟资金
        self.__current = self.__broker.current  # 当前动态权益
        self.__pre_balance = self.__broker.pre_balance  # 静态权益
        self.__cash = self.__broker.cash  # 可用资金
        self.__shares = dict()  # { instrument : position }
        self.__cur_account = None
        self.__margin = 0  # 占用保证金
        self.__activeOrders = {}  # 未成交委托单
        self.__re_extract_code = re.compile(r'([a-zA-Z]*)(\d+)')  # 提合约字母部分 IF1509 -> IF
        self.__re_extract_name = re.compile('(.*?)([0-9]+)(.*?)$')  # 提取合约文字部分
        self.__trading_day = timezone.make_aware(
            datetime.datetime.strptime(self.raw_redis.get("TradingDay")+'08', '%Y%m%d%H'))
        self.__last_trading_day = timezone.make_aware(
            datetime.datetime.strptime(self.raw_redis.get("LastTradingDay")+'08', '%Y%m%d%H'))

    async def start(self):
        await self.install()
        # await self.processing_signal2()
        self.raw_redis.set('HEARTBEAT:TRADER', 1, ex=61)
        now = int(timezone.localtime().strftime('%H%M'))
        if 840 <= now <= 1550 or 2010 <= now <= 2359:  # 非交易时间查不到数据
            await self.refresh_account()
            order_list = await self.query('Order')
            for order in order_list:
                # 未成交订单
                if int(order['OrderStatus']) in range(1, 5) and \
                        order['OrderSubmitStatus'] == ApiStruct.OSS_Accepted:
                    direct_str = DirectionType.values[order['Direction']]
                    logger.info(f"撤销未成交订单: 合约{order['InstrumentID']} {direct_str}单 {order['VolumeTotal']}手 "
                                f"价格{order['LimitPrice']}")
                    await self.cancel_order(order)
                # 已成交订单
                else:
                    self.save_order(order)
            await self.refresh_position()

    async def refresh_account(self):
        try:
            account = await self.query('TradingAccount')
            # 静态权益=上日结算-出金金额+入金金额
            self.__pre_balance = Decimal(account['PreBalance']) - Decimal(
                account['Withdraw']) + Decimal(account['Deposit'])
            # 动态权益=静态权益+平仓盈亏+持仓盈亏-手续费
            self.__current = self.__pre_balance + Decimal(account['CloseProfit']) + Decimal(
                account['PositionProfit']) - Decimal(account['Commission'])
            self.__margin = Decimal(account['CurrMargin'])
            self.__cash = Decimal(account['Available'])
            self.__cur_account = account
            self.__broker.cash = self.__cash
            self.__broker.current = self.__current
            self.__broker.pre_balance = self.__pre_balance
            self.__broker.save(update_fields=['cash', 'current', 'pre_balance'])
            logger.debug(f"更新账户,可用资金: {self.__cash:,.0f} 静态权益: {self.__pre_balance:,.0f} "
                         f"动态权益: {self.__current:,.0f} 虚拟: {self.__fake:,.0f}")
            self.__strategy = self.__broker.strategy_set.first()
            self.__inst_ids = [inst.product_code for inst in self.__strategy.instruments.all()]
        except Exception as e:
            logger.warning(f'refresh_account 发生错误: {repr(e)}', exc_info=True)
    
    async def refresh_position(self):
        try:
            pos_list = await self.query('InvestorPositionDetail')
            shares_dict = {}
            for pos in pos_list:
                if 'empty' in pos and pos['empty'] is True:
                    continue
                if pos['Volume'] > 0:
                    old_pos = shares_dict.get(pos['InstrumentID'])
                    if old_pos is None:
                        shares_dict[pos['InstrumentID']] = pos
                    else:
                        old_pos['OpenPrice'] = (old_pos['OpenPrice'] * old_pos['Volume'] +
                                                pos['OpenPrice'] * pos['Volume']) / (old_pos['Volume'] + pos['Volume'])
                        old_pos['Volume'] += pos['Volume']
                        old_pos['PositionProfitByTrade'] += pos['PositionProfitByTrade']
                        old_pos['Margin'] += pos['Margin']
            Trade.objects.exclude(code__in=shares_dict.keys()).delete()  # 删除不存在的头寸
            for _, pos in shares_dict.items():
                p_code = self.__re_extract_code.match(pos['InstrumentID']).group(1)
                inst = Instrument.objects.get(product_code=p_code)
                profit = pos['PositionProfitByTrade']
                trade = Trade.objects.filter(
                    broker=self.__broker, strategy=self.__strategy, instrument=inst, code=pos['InstrumentID'],
                    direction=DirectionType.values[pos['Direction']], close_time__isnull=True).first()
                if trade:
                    trade.shares = (trade.closed_shares if trade.closed_shares else 0) + pos['Volume']
                    trade.filled_shares = trade.shares
                    trade.save(update_fields=['shares', 'filled_shares', 'profit'])
                else:
                    Trade.objects.create(
                        broker=self.__broker, strategy=self.__strategy, instrument=inst, code=pos['InstrumentID'],
                        direction=DirectionType.values[pos['Direction']],
                        open_time=timezone.make_aware(datetime.datetime.strptime(pos['OpenDate'] + '08', '%Y%m%d%H')),
                        shares=pos['Volume'], filled_shares=pos['Volume'], avg_entry_price=Decimal(pos['OpenPrice']),
                        cost=pos['Volume'] * Decimal(pos['OpenPrice']) * inst.fee_money * inst.volume_multiple + pos[
                            'Volume'] * inst.fee_volume, profit=profit, frozen_margin=Decimal(pos['Margin']))
        except Exception as e:
            logger.warning(f'refresh_position 发生错误: {repr(e)}', exc_info=True)
    
    async def refresh_instrument(self):
        try:
            inst_dict = defaultdict(dict)
            inst_list = await self.query('Instrument')
            for inst in inst_list:
                if inst['empty']:
                    continue
                if inst['IsTrading'] == 1 and chr(inst['ProductClass']) == ApiStruct.PC_Futures and \
                        int(str_to_number(inst['StrikePrice'])) == 0:
                    if inst['ProductID'] in self.__ignore_inst_list or inst['LongMarginRatio'] > 1:
                        continue
                    inst_dict[inst['ProductID']][inst['InstrumentID']] = dict()
                    inst_dict[inst['ProductID']][inst['InstrumentID']]['name'] = inst['InstrumentName']
                    inst_dict[inst['ProductID']][inst['InstrumentID']]['exchange'] = inst['ExchangeID']
                    inst_dict[inst['ProductID']][inst['InstrumentID']]['multiple'] = inst['VolumeMultiple']
                    inst_dict[inst['ProductID']][inst['InstrumentID']]['price_tick'] = inst['PriceTick']
                    inst_dict[inst['ProductID']][inst['InstrumentID']]['margin'] = inst['LongMarginRatio']
            for code in inst_dict.keys():
                all_inst = ','.join(sorted(inst_dict[code].keys()))
                inst_data = list(inst_dict[code].values())[0]
                valid_name = self.__re_extract_name.match(inst_data['name'])
                if valid_name is not None:
                    valid_name = valid_name.group(1)
                else:
                    valid_name = inst_data['name']
                if valid_name == code:
                    valid_name = ''
                inst_data['name'] = valid_name
                inst, created = Instrument.objects.update_or_create(product_code=code, exchange=inst_data['exchange'])
                print(f"inst:{inst} created:{created} main_code:{inst.main_code}")
                if created:
                    inst.name = inst_data['name']
                    inst.volume_multiple = inst_data['multiple']
                    inst.price_tick = inst_data['price_tick']
                    inst.margin_rate = inst_data['margin']
                    inst.save(update_fields=['name', 'volume_multiple', 'price_tick', 'margin_rate'])
                elif inst.main_code:
                    inst.margin_rate = inst_dict[code][inst.main_code]['margin']
                    inst.all_inst = all_inst
                    inst.save(update_fields=['margin_rate', 'all_inst'])
        except Exception as e:
            logger.warning(f'refresh_instrument 发生错误: {repr(e)}', exc_info=True)

    async def refresh_fee(self):
        try:
            for inst in Instrument.objects.filter(main_code__isnull=False):
                fee = await self.query('InstrumentCommissionRate', InstrumentID=inst.main_code)
                fee = fee[0]
                inst.fee_money = Decimal(fee['CloseRatioByMoney'])
                inst.fee_volume = Decimal(fee['CloseRatioByVolume'])
                inst.save(update_fields=['fee_money', 'fee_volume'])
                logger.debug(f"{inst} 已更新手续费")
        except Exception as e:
            logger.warning(f'refresh_fee 发生错误: {repr(e)}', exc_info=True)

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
            shares += pos['Volume'] * (-1 if pos['Direction'] == DirectionType.SHORT else 1)
        return shares, pos_price / abs(shares), self.__shares[instrument][0]['OpenDate']

    def getPositions(self, inst_id: int):
        # 这个函数只能处理持有单一方向仓位的情况，若同时持有多空的头寸，返回结果不正确
        return self.__shares[inst_id][0]

    def async_query(self, query_type: str, **kwargs):
        request_id = self.next_id()
        kwargs['RequestID'] = request_id
        self.raw_redis.publish(self.__request_format.format('ReqQry' + query_type), json.dumps(kwargs))

    @staticmethod
    async def query_reader(pb: aioredis.client.PubSub):
        msg_list = []
        async for msg in pb.listen():
            # print(f"query_reader msg: {msg}")
            msg_dict = json.loads(msg['data'])
            if 'empty' not in msg_dict or not msg_dict['empty']:
                msg_list.append(msg_dict)
            if 'bIsLast' not in msg_dict or msg_dict['bIsLast']:
                return msg_list

    async def query(self, query_type: str, **kwargs):
        sub_client = None
        channel_rsp_qry, channel_rsp_err = None, None
        try:
            sub_client = self.redis_client.pubsub(ignore_subscribe_messages=True)
            request_id = self.next_id()
            kwargs['RequestID'] = request_id
            channel_rsp_qry = self.__trade_response_format.format('OnRspQry' + query_type, request_id)
            channel_rsp_err = self.__trade_response_format.format('OnRspError', request_id)
            await sub_client.psubscribe(channel_rsp_qry, channel_rsp_err)
            task = asyncio.create_task(self.query_reader(sub_client))
            self.raw_redis.publish(self.__request_format.format('ReqQry' + query_type), json.dumps(kwargs))
            await asyncio.wait_for(task, HANDLER_TIME_OUT)
            await sub_client.punsubscribe()
            await sub_client.close()
            return task.result()
        except Exception as e:
            logger.warning(f'{query_type} 发生错误: {repr(e)}', exc_info=True)
            if sub_client and channel_rsp_qry:
                await sub_client.unsubscribe()
                await sub_client.close()
            return None

    async def SubscribeMarketData(self, inst_ids: list):
        sub_client = None
        channel_rsp_dat, channel_rsp_err = None, None
        try:
            sub_client = self.redis_client.pubsub(ignore_subscribe_messages=True)
            channel_rsp_dat = self.__market_response_format.format('OnRspSubMarketData', 0)
            channel_rsp_err = self.__market_response_format.format('OnRspError', 0)
            await sub_client.psubscribe(channel_rsp_dat, channel_rsp_err)
            task = asyncio.create_task(self.query_reader(sub_client))
            self.raw_redis.publish(self.__request_format.format('SubscribeMarketData'), json.dumps(inst_ids))
            await asyncio.wait_for(task, HANDLER_TIME_OUT)
            await sub_client.punsubscribe()
            await sub_client.close()
            return task.result()
        except Exception as e:
            logger.warning(f'SubscribeMarketData 发生错误: {repr(e)}', exc_info=True)
            if sub_client and sub_client.in_pubsub and channel_rsp_dat:
                await sub_client.unsubscribe()
                await sub_client.close()
            return None

    async def UnSubscribeMarketData(self, inst_ids: list):
        sub_client = None
        channel_rsp_dat, channel_rsp_err = None, None
        try:
            sub_client = self.redis_client.pubsub(ignore_subscribe_messages=True)
            channel_rsp_dat = self.__market_response_format.format('OnRspUnSubMarketData', 0)
            channel_rsp_err = self.__market_response_format.format('OnRspError', 0)
            await sub_client.psubscribe(channel_rsp_dat, channel_rsp_err)
            task = asyncio.create_task(self.query_reader(sub_client))
            self.raw_redis.publish(self.__request_format.format('UnSubscribeMarketData'), json.dumps(inst_ids))
            await asyncio.wait_for(task, HANDLER_TIME_OUT)
            await sub_client.punsubscribe()
            await sub_client.close()
            return task.result()
        except Exception as e:
            logger.warning(f'UnSubscribeMarketData 发生错误: {repr(e)}', exc_info=True)
            if sub_client and sub_client.in_pubsub and channel_rsp_dat:
                await sub_client.unsubscribe()
                await sub_client.close()
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
        close_flag = ApiStruct.OF_Close
        if pos.open_time.date() == timezone.localtime().date() and pos.instrument.exchange == ExchangeType.SHFE:
            close_flag = ApiStruct.OF_CloseToday  # 上期所区分平今和平昨
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
        close_flag = ApiStruct.OF_Close
        if pos.open_time.date() == timezone.localtime().date() and pos.instrument.exchange == ExchangeType.SHFE:
            close_flag = ApiStruct.OF_CloseToday  # 上期所区分平今和平昨
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
        channel_rtn_odr, channel_rsp_err = None, None
        try:
            sub_client = self.redis_client.pubsub(ignore_subscribe_messages=True)
            request_id = self.next_id()
            order_ref = self.next_order_ref()
            kwargs['RequestID'] = request_id
            kwargs['OrderRef'] = order_ref
            channel_rtn_odr = self.__trade_response_format.format('OnRtnOrder', order_ref)
            channel_rsp_err = self.__trade_response_format.format('OnRspError', request_id)
            channel_rsp_odr = self.__trade_response_format.format('OnRspOrderInsert', 0)
            await sub_client.psubscribe(channel_rtn_odr, channel_rsp_err, channel_rsp_odr)
            task = asyncio.create_task(self.query_reader(sub_client))
            self.raw_redis.publish(self.__request_format.format('ReqOrderInsert'), json.dumps(kwargs))
            await asyncio.wait_for(task, HANDLER_TIME_OUT)
            await sub_client.punsubscribe()
            await sub_client.close()
            result = task.result()[0]
            if 'ErrorID' in result:
                logger.warning(f"提交订单出错: {ctp_errors[result['ErrorID']]}")
                return False
            logger.debug(f"ReqOrderInsert, rst: {result['StatusMsg']}")
            return result
        except Exception as e:
            logger.warning(f'ReqOrderInsert 发生错误: {repr(e)}', exc_info=True)
            if sub_client and sub_client.in_pubsub and channel_rtn_odr:
                await sub_client.unsubscribe()
                await sub_client.close()
            return False

    async def cancel_order(self, order: dict):
        sub_client = None
        channel_rsp_odr_act, channel_rsp_err = None, None
        try:
            sub_client = self.redis_client.pubsub(ignore_subscribe_messages=True)
            request_id = self.next_id()
            order['RequestID'] = request_id
            channel_rtn_odr = self.__trade_response_format.format('OnRtnOrder', order['OrderRef'])
            channel_rsp_odr_act = self.__trade_response_format.format('OnRspOrderAction', 0)
            channel_rsp_err = self.__trade_response_format.format('OnRspError', request_id)
            await sub_client.psubscribe(channel_rtn_odr, channel_rsp_odr_act, channel_rsp_err)
            task = asyncio.create_task(self.query_reader(sub_client))
            self.raw_redis.publish(self.__request_format.format('ReqOrderAction'), json.dumps(order))
            await asyncio.wait_for(task, HANDLER_TIME_OUT)
            await sub_client.punsubscribe()
            await sub_client.close()
            result = task.result()[0]
            if 'ErrorID' in result:
                logger.warning(f"撤销订单出错: {ctp_errors[result['ErrorID']]}")
                return False
            return True
        except Exception as e:
            logger.warning('cancel_order 发生错误: %s', repr(e), exc_info=True)
            if sub_client and sub_client.in_pubsub and channel_rsp_odr_act:
                await sub_client.unsubscribe()
                await sub_client.close()
            return False

    @RegisterCallback(channel='MSG:CTP:RSP:MARKET:OnRtnDepthMarketData:*')
    async def OnRtnDepthMarketData(self, channel, tick: dict):
        try:
            inst = channel.split(':')[-1]
            tick['UpdateTime'] = datetime.datetime.strptime(tick['UpdateTime'], "%Y%m%d %H:%M:%S:%f")
            logger.debug('inst=%s, tick: %s', inst, tick)
        except Exception as ee:
            logger.warning('OnRtnDepthMarketData 发生错误: %s', repr(ee), exc_info=True)

    @staticmethod
    def get_trade_string(trade: dict) -> str:
        return f"成交订单: {trade['OrderRef']}, {trade['ExchangeID']}.{trade['InstrumentID']} " \
               f"{OffsetFlag.values[trade['OffsetFlag']]}{DirectionType.values[trade['Direction']]}" \
               f"已成交{trade['Volume']}手 价格:{trade['Price']:.3} 时间:{trade['TradeTime']}"

    @RegisterCallback(channel='MSG:CTP:RSP:TRADE:OnRtnTrade:*')
    async def OnRtnTrade(self, channel, trade: dict):
        try:
            signal = None
            order_ref = channel.split(':')[-1]
            logger.info(f"成交回报: {self.get_trade_string(trade)}")
            inst = Instrument.objects.get(product_code=self.__re_extract_code.match(trade['InstrumentID']).group(1))
            order = Order.objects.filter(order_ref=order_ref, code=trade['InstrumentID']).order_by('-send_time').first()
            trade_cost = trade['Volume'] * Decimal(trade['Price']) * inst.fee_money * inst.volume_multiple + \
                trade['Volume'] * inst.fee_volume
            trade_margin = trade['Volume'] * Decimal(trade['Price']) * inst.margin_rate
            trade_time = timezone.make_aware(
                datetime.datetime.strptime(trade['TradeDate'] + trade['TradeTime'], '%Y%m%d%H:%M:%S'))
            new_trade = False
            manual_trade = False
            trade_completed = False
            if trade['OffsetFlag'] == OffsetFlag.Open:  # 开仓
                last_trade = Trade.objects.filter(
                    broker=self.__broker, strategy=self.__strategy, instrument=inst, code=trade['InstrumentID'],
                    # open_order=order,
                    open_time__lte=trade_time, close_time__isnull=True,
                    direction=DirectionType.values[trade['Direction']]).first()
                # print(connection.queries[-1]['sql'])
                if last_trade is None:
                    new_trade = True
                    last_trade = Trade.objects.create(
                        broker=self.__broker, strategy=self.__strategy, instrument=inst, code=trade['InstrumentID'],
                        open_order=order if order else None,
                        direction=DirectionType.values[trade['Direction']],
                        open_time=trade_time, shares=order.volume if order else trade['Volume'], cost=trade_cost,
                        filled_shares=trade['Volume'], avg_entry_price=trade['Price'], frozen_margin=trade_margin)
                else:
                    if last_trade.shares == last_trade.filled_shares:
                        manual_trade = True
                if order is None or order.volume == trade['Volume']:
                    trade_completed = True
                if (not new_trade and not manual_trade) or (trade_completed and not new_trade and manual_trade):
                    last_trade.avg_entry_price = \
                        (last_trade.avg_entry_price * last_trade.filled_shares + trade['Volume'] *
                         Decimal(trade['Price'])) / (last_trade.filled_shares + trade['Volume'])
                    last_trade.filled_shares += trade['Volume']
                    if trade_completed and not new_trade and manual_trade:
                        last_trade.shares += trade['Volume']
                    last_trade.cost += trade_cost
                    last_trade.frozen_margin += trade_margin
                    last_trade.save()
                if trade_completed:
                    if manual_trade:
                        signal = Signal.objects.filter(
                            type=SignalType.BUY if trade['Direction'] == DirectionType.LONG else SignalType.SELL_SHORT)
                    else:
                        signal = Signal.objects.filter(
                            Q(type=SignalType.BUY if trade['Direction'] == DirectionType.LONG else
                                SignalType.SELL_SHORT) | Q(type=SignalType.ROLL_OPEN))
                    signal = signal.filter(
                        code=trade['InstrumentID'], trigger_time__gte=self.__last_trading_day, volume=trade['Volume'],
                        strategy=self.__strategy, instrument=inst, processed=False).first()
            else:  # 平仓 TODO: 部分成交的情况下，最后一笔成交完成时程序不会修改trade表对应的仓位
                open_direct = DirectionType.values[DirectionType.LONG] if \
                    trade['Direction'] == DirectionType.SHORT else DirectionType.values[DirectionType.SHORT]
                last_trade = Trade.objects.filter(
                    Q(closed_shares__isnull=True) | Q(closed_shares__lt=F('shares')), shares=F('filled_shares'),
                    broker=self.__broker, strategy=self.__strategy, instrument=inst, code=trade['InstrumentID'],
                    open_time__lte=self.__trading_day, direction=open_direct).first()
                print(connection.queries[-1]['sql'])
                print(f'trade={last_trade}')
                if last_trade:
                    if last_trade.closed_shares and last_trade.avg_exit_price:
                        last_trade.avg_exit_price = (last_trade.avg_exit_price * last_trade.closed_shares + trade[
                            'Volume'] * Decimal(trade['Price'])) / (last_trade.closed_shares + trade['Volume'])
                        last_trade.closed_shares += trade['Volume']
                    else:
                        last_trade.avg_exit_price = trade['Volume'] * Decimal(trade['Price']) / trade['Volume']
                        last_trade.closed_shares = trade['Volume']
                    last_trade.cost += trade_cost
                    if last_trade.closed_shares == last_trade.shares:  # 全部成交
                        trade_completed = True
                        last_trade.close_order = order
                        last_trade.close_time = timezone.make_aware(datetime.datetime.strptime(
                            trade['TradeDate'] + trade['TradeTime'], '%Y%m%d%H:%M:%S'))
                        if last_trade.direction == DirectionType.values[DirectionType.LONG]:
                            profit_point = last_trade.avg_exit_price - last_trade.avg_entry_price
                        else:
                            profit_point = last_trade.avg_entry_price - last_trade.avg_exit_price
                        last_trade.profit = profit_point * last_trade.shares * inst.volume_multiple
                    last_trade.save(force_update=True)
                else:
                    manual_trade = True
                if trade_completed:
                    if manual_trade:
                        signal = Signal.objects.filter(
                            type=SignalType.BUY_COVER if trade['Direction'] == DirectionType.LONG else SignalType.SELL)
                    else:
                        signal = Signal.objects.filter(
                            Q(type=SignalType.BUY_COVER if trade['Direction'] == DirectionType.LONG else
                                SignalType.SELL) | Q(type=SignalType.ROLL_CLOSE))
                    signal = signal.filter(
                        code=trade['InstrumentID'], trigger_time__gte=self.__last_trading_day, volume=trade['Volume'],
                        strategy=self.__strategy, instrument=inst, processed=False).first()
            logger.debug(f"new_trade:{new_trade} manual_trade:{manual_trade} "
                         f"trade_completed:{trade_completed} order:{order}")
            logger.debug(f"signal: {signal}")
            if signal:
                signal.processed = True
                signal.save(update_fields=['processed'])
        except Exception as ee:
            logger.warning(f'OnRtnTrade 发生错误: {repr(ee)}', exc_info=True)

    def save_order(self, order: dict):
        product_code = self.__re_extract_code.match(order['InstrumentID']).group(1)
        inst = Instrument.objects.get(product_code=product_code)
        order_ref = Decimal(order['OrderRef'])
        if order_ref < 100000:  # 非本程序生成订单
            return None, None
        return Order.objects.update_or_create(order_ref=order['OrderRef'], code=order['InstrumentID'], defaults={
            'broker': self.__broker, 'strategy': self.__strategy, 'instrument': inst, 'front': order['FrontID'],
            'session': order['SessionID'], 'price': order['LimitPrice'], 'volume': order['VolumeTotalOriginal'],
            'direction': DirectionType.values[order['Direction']], 'status': OrderStatus.values[order['OrderStatus']],
            'offset_flag': CombOffsetFlag.values[order['CombOffsetFlag']],
            'send_time': timezone.make_aware(
                datetime.datetime.strptime(order['InsertDate'] + order['InsertTime'], '%Y%m%d%H:%M:%S')),
            'update_time': timezone.localtime()})

    @staticmethod
    def get_order_string(order: dict) -> str:
        off_set_flag = CombOffsetFlag.values[order['CombOffsetFlag']] if order['CombOffsetFlag'] in \
                                                                         CombOffsetFlag.values else OffsetFlag.values[
            order['CombOffsetFlag']]
        order_str = f"订单号:{order['OrderRef']},{order['ExchangeID']}.{order['InstrumentID']} " \
                    f"{off_set_flag}{DirectionType.values[order['Direction']]}" \
                    f"{order['VolumeTotalOriginal']}手 价格:{order['LimitPrice']} 报单时间:{order['InsertTime']} " \
                    f"提交状态:{OrderSubmitStatus.values[order['OrderSubmitStatus']]} "
        if order['OrderStatus'] != OrderStatus.Unknown:
            order_str += f"成交状态:{OrderStatus.values[order['OrderStatus']]} 消息:{order['StatusMsg']} "
            if order['OrderStatus'] == OrderStatus.PartTradedQueueing:
                order_str += f"成交数量:{order['VolumeTraded']} 剩余数量:{order['VolumeTotal']}"
        return order_str

    @RegisterCallback(channel='MSG:CTP:RSP:TRADE:OnRtnOrder:*')
    async def OnRtnOrder(self, _: str, order: dict):
        try:
            if order["OrderSysID"]:
                logger.debug(f"订单回报: {self.get_order_string(order)}")
            order_obj, _ = self.save_order(order)
            inst = Instrument.objects.get(product_code=self.__re_extract_code.match(order['InstrumentID']).group(1))
            # 处理由于委托价格超出交易所涨跌停板而被撤单的报单，将委托价格下调50%，重新报单
            if order['OrderStatus'] == OrderStatus.Canceled and \
                    order['OrderSubmitStatus'] == OrderSubmitStatus.InsertRejected:
                last_bar = DailyBar.objects.filter(
                    exchange=inst.exchange, code=order['InstrumentID']).order_by('-time').first()
                volume = int(order['VolumeTotalOriginal'])
                price = Decimal(order['LimitPrice'])
                if order['CombOffsetFlag'] == CombOffsetFlag.Open:
                    if order['Direction'] == DirectionType.LONG:
                        delta = (price - last_bar.settlement) * Decimal(0.5)
                        price = price_round(last_bar.settlement + delta, inst.price_tick)
                        if delta / last_bar.settlement < 0.01:
                            logger.warning(f"{inst} 新价格: {price} 过低难以成交，放弃报单!")
                            return
                        logger.info(f"{inst} 以价格 {price} 开多{volume}手 重新报单...")
                        self.io_loop.create_task(self.buy(inst, price, volume))
                    else:
                        delta = (last_bar.settlement - price) * Decimal(0.5)
                        price = price_round(last_bar.settlement - delta, inst.price_tick)
                        if delta / last_bar.settlement < 0.01:
                            logger.warning(f"{inst} 新价格: {price} 过低难以成交，放弃报单!")
                            return
                        logger.info(f"{inst} 以价格 {price} 开空{volume}手 重新报单...")
                        self.io_loop.create_task(self.sell_short(inst, price, volume))
                else:
                    pos = Trade.objects.filter(
                        close_time__isnull=True, code=order['InstrumentID'], broker=self.__broker,
                        strategy=self.__strategy, instrument=inst, shares__gt=0).first()
                    if order['Direction'] == DirectionType.LONG:
                        delta = (price - last_bar.settlement) * Decimal(0.5)
                        price = price_round(last_bar.settlement + delta, inst.price_tick)
                        if delta / last_bar.settlement < 0.01:
                            logger.warning(f"{inst} 新价格: {price} 过低难以成交，放弃报单!")
                            return
                        logger.info(f"{inst} 以价格 {price} 买平{volume}手 重新报单...")
                        self.io_loop.create_task(self.buy_cover(pos, price, volume))
                    else:
                        delta = (last_bar.settlement - price) * Decimal(0.5)
                        price = price_round(last_bar.settlement - delta, inst.price_tick)
                        if delta / last_bar.settlement < 0.01:
                            logger.warning(f"{inst} 新价格: {price} 过低难以成交，放弃报单!")
                            return
                        logger.info(f"{inst} 以价格 {price} 卖平{volume}手 重新报单...")
                        self.io_loop.create_task(self.sell(pos, price, volume))
        except Exception as ee:
            logger.warning(f'OnRtnOrder 发生错误: {repr(ee)}', exc_info=True)

    @RegisterCallback(crontab='*/1 * * * *')
    async def heartbeat(self):
        self.raw_redis.set('HEARTBEAT:TRADER', 1, ex=301)

    @RegisterCallback(crontab='55 8 * * *')
    async def processing_signal1(self):
        day = timezone.localtime()
        _, trading = await is_trading_day(day)
        if trading:
            logger.debug('查询日盘信号..')
            for sig in Signal.objects.filter(
                    ~Q(instrument__exchange=ExchangeType.CFFEX), trigger_time__gte=self.__last_trading_day,
                    strategy=self.__strategy, instrument__night_trade=False, processed=False).all():
                logger.info(f'发现日盘信号: {sig}')
                self.process_signal(sig)

    @RegisterCallback(crontab='1 9 * * *')
    async def check_signal1_processed(self):
        day = timezone.localtime()
        _, trading = await is_trading_day(day)
        if trading:
            logger.debug('查询遗漏的日盘信号..')
            for sig in Signal.objects.filter(
                    ~Q(instrument__exchange=ExchangeType.CFFEX), trigger_time__gte=self.__last_trading_day,
                    strategy=self.__strategy, instrument__night_trade=False, processed=False).all():
                logger.info(f'发现遗漏信号: {sig}')
                self.process_signal(sig)

    @RegisterCallback(crontab='25 9 * * *')
    async def processing_signal2(self):
        day = timezone.localtime()
        _, trading = await is_trading_day(day)
        if trading:
            logger.debug('查询股指和国债信号..')
            for sig in Signal.objects.filter(
                    instrument__exchange=ExchangeType.CFFEX, trigger_time__gte=self.__last_trading_day,
                    strategy=self.__strategy, instrument__night_trade=False, processed=False).all():
                logger.info(f'发现股指和国债信号: {sig}')
                self.process_signal(sig)

    @RegisterCallback(crontab='31 9 * * *')
    async def check_signal2_processed(self):
        day = timezone.localtime()
        _, trading = await is_trading_day(day)
        if trading:
            logger.debug('查询遗漏的股指和国债信号..')
            for sig in Signal.objects.filter(
                    instrument__exchange=ExchangeType.CFFEX, trigger_time__gte=self.__last_trading_day,
                    strategy=self.__strategy, instrument__night_trade=False, processed=False).all():
                logger.info(f'发现遗漏的股指和国债信号: {sig}')
                self.process_signal(sig)

    @RegisterCallback(crontab='55 20 * * *')
    async def processing_signal3(self):
        day = timezone.localtime()
        _, trading = await is_trading_day(day)
        if trading:
            logger.debug('查询夜盘信号..')
            for sig in Signal.objects.filter(
                    trigger_time__gte=self.__last_trading_day,
                    strategy=self.__strategy, instrument__night_trade=True, processed=False).all():
                logger.info(f'发现夜盘信号: {sig}')
                self.process_signal(sig)

    @RegisterCallback(crontab='1 21 * * *')
    async def check_signal3_processed(self):
        day = timezone.localtime()
        _, trading = await is_trading_day(day)
        if trading:
            logger.debug('查询遗漏的夜盘信号..')
            for sig in Signal.objects.filter(
                    trigger_time__gte=self.__last_trading_day,
                    strategy=self.__strategy, instrument__night_trade=True, processed=False).all():
                logger.info(f'发现遗漏的夜盘信号: {sig}')
                self.process_signal(sig)

    @RegisterCallback(crontab='20 15 * * *')
    async def refresh_all(self):
        day = timezone.localtime()
        _, trading = await is_trading_day(day)
        if not trading:
            logger.info('今日是非交易日, 不更新任何数据。')
            return
        logger.debug('更新账户')
        await self.refresh_account()
        logger.debug('更新合约数据..')
        await self.refresh_instrument()
        await self.refresh_fee()
        logger.debug('更新持仓')
        await self.refresh_position()
        logger.debug('更新手续费')
        await self.refresh_fee()
        logger.debug('全部更新完成!')

    @RegisterCallback(crontab='30 15 * * *')
    async def update_equity(self):
        today, trading = await is_trading_day(timezone.localtime())
        if trading:
            logger.info(f"更新净值，可用资金: {self.__cash:,.0f} 静态权益: {self.__pre_balance:,.0f} "
                        f"动态权益: {self.__current:,.0f} 虚拟: {self.__fake:,.0f}")
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
            nav = (self.__current + self.__fake) / unit
            accumulated = (self.__current + self.__fake - dividend) / (unit - dividend)
            Performance.objects.update_or_create(broker=self.__broker, day=today.date(), defaults={
                'used_margin': self.__margin,
                'capital': self.__current, 'unit_count': unit, 'NAV': nav, 'accumulated': accumulated})

    @RegisterCallback(crontab='0 17 * * *')
    async def collect_quote(self, tasks=None):
        try:
            day = timezone.localtime()
            _, trading = await is_trading_day(day)
            if not trading:
                logger.info('今日是非交易日, 不计算任何数据。')
                return
            logger.debug(f'{day}盘后计算,获取交易所日线数据..')
            if tasks is None:
                tasks = [update_from_shfe, update_from_dce, update_from_czce, update_from_cffex, get_contracts_argument]
            result = await asyncio.gather(*[func(day) for func in tasks], return_exceptions=True)
            if all(result):
                self.io_loop.call_soon(self.calculate, day)
            else:
                failed_tasks = [tasks[i] for i, rst in enumerate(result) if not rst]
                self.io_loop.call_later(10, asyncio.create_task, self.collect_quote(failed_tasks))
        except Exception as e:
            logger.warning(f'collect_quote 发生错误: {repr(e)}', exc_info=True)
        logger.debug('盘后计算完毕!')

    def calculate(self, day):
        try:
            for inst_obj in Instrument.objects.all():
                logger.debug(f'计算连续合约, 交易信号: {inst_obj.name}')
                calc_main_inst(inst_obj, day)
                if inst_obj.product_code in self.__inst_ids:
                    self.calc_signal(inst_obj, day)
        except Exception as e:
            logger.warning(f'calculate 发生错误: {repr(e)}', exc_info=True)

    def calc_signal(self, inst: Instrument, day: datetime.datetime):
        try:
            if inst.product_code not in self.__inst_ids:
                return
            break_n = self.__strategy.param_set.get(code='BreakPeriod').int_value
            atr_n = self.__strategy.param_set.get(code='AtrPeriod').int_value
            long_n = self.__strategy.param_set.get(code='LongPeriod').int_value
            short_n = self.__strategy.param_set.get(code='ShortPeriod').int_value
            stop_n = self.__strategy.param_set.get(code='StopLoss').int_value
            risk = self.__strategy.param_set.get(code='Risk').float_value
            df = to_df(MainBar.objects.filter(
                time__lte=day.date(),
                exchange=inst.exchange, product_code=inst.product_code).order_by('time').values_list(
                'time', 'open', 'high', 'low', 'close', 'settlement'))
            df.index = pd.DatetimeIndex(df.time)
            df['atr'] = ATR(df.open, df.high, df.low, timeperiod=atr_n)
            df['short_trend'] = df.close
            df['long_trend'] = df.close
            # df columns: 0:time,1:open,2:high,3:low,4:close,5:settlement,6:atr,7:short_trend,8:long_trend
            for idx in range(1, df.shape[0]):
                df.iloc[idx, 7] = (df.iloc[idx - 1, 7] * (short_n - 1) + df.iloc[idx, 4]) / short_n
                df.iloc[idx, 8] = (df.iloc[idx - 1, 8] * (long_n - 1) + df.iloc[idx, 4]) / long_n
            df['high_line'] = df.close.rolling(window=break_n).max()
            df['low_line'] = df.close.rolling(window=break_n).min()
            idx = -1
            pos_idx = None
            buy_sig = df.short_trend[idx] > df.long_trend[idx] and int(df.close[idx]) >= int(df.high_line[idx - 1])
            sell_sig = df.short_trend[idx] < df.long_trend[idx] and int(df.close[idx]) <= int(df.low_line[idx - 1])
            pos = Trade.objects.filter(
                Q(close_time__isnull=True) | Q(close_time__gt=day),
                broker=self.__broker, strategy=self.__strategy,
                instrument=inst, shares__gt=0, open_time__lt=day).first()
            roll_over = False
            if pos:
                pos_idx = df.index.get_loc(
                    pos.open_time.astimezone(pytz.FixedOffset(480)).date().isoformat())
                roll_over = pos.code != inst.main_code
            elif self.__strategy.force_opens.filter(id=inst.id).exists() and not buy_sig and not sell_sig:
                logger.info(f'强制开仓: {inst}')
                if df.short_trend[idx] > df.long_trend[idx]:
                    buy_sig = True
                else:
                    sell_sig = True
                self.__strategy.force_opens.remove(inst)
            signal = None
            signal_value = None
            price = None
            volume = None
            if pos:
                # 多头持仓
                if pos.direction == DirectionType.values[DirectionType.LONG]:
                    hh = float(MainBar.objects.filter(
                        exchange=inst.exchange, product_code=pos.instrument.product_code,
                        time__gte=pos.open_time.date(), time__lte=day).aggregate(Max('high'))['high__max'])
                    # 多头止损
                    if df.close[idx] <= hh - df.atr[pos_idx - 1] * stop_n:
                        signal = SignalType.SELL
                        # 止损时 signal_value 为止损价
                        signal_value = hh - df.atr[pos_idx - 1] * stop_n
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
                    if df.close[idx] >= ll + df.atr[pos_idx - 1] * stop_n:
                        signal = SignalType.BUY_COVER
                        signal_value = ll + df.atr[pos_idx - 1] * stop_n
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
                volume = (self.__current + self.__fake) * risk // \
                         (Decimal(df.atr[idx]) * Decimal(inst.volume_multiple))
                if volume > 0:
                    signal = SignalType.BUY
                    signal_value = df.high_line[idx - 1]
                    new_bar = DailyBar.objects.filter(
                        exchange=inst.exchange, code=inst.main_code, time=day.date()).first()
                    price = self.calc_up_limit(inst, new_bar)
                else:
                    logger.info(f'做多单手风险={df.atr[idx] * inst.volume_multiple},超出风控额度，放弃。')
            # 做空
            elif sell_sig:
                volume = (self.__current + self.__fake) * risk // \
                         (Decimal(df.atr[idx]) * Decimal(inst.volume_multiple))
                if volume > 0:
                    signal = SignalType.SELL_SHORT
                    signal_value = df.low_line[idx - 1]
                    new_bar = DailyBar.objects.filter(
                        exchange=inst.exchange, code=inst.main_code, time=day.date()).first()
                    price = self.calc_down_limit(inst, new_bar)
                else:
                    logger.info(f'做空单手风险={df.atr[idx] * inst.volume_multiple},超出风控额度，放弃。')
            if signal:
                sig, _ = Signal.objects.update_or_create(
                    code=inst.main_code,
                    strategy=self.__strategy, instrument=inst, type=signal, trigger_time=day, defaults={
                        'price': price, 'volume': volume, 'trigger_value': signal_value,
                        'priority': PriorityType.Normal, 'processed': False})
                logger.info(f"新信号：{sig}")
        except Exception as e:
            logger.warning(f'calc_signal 发生错误: {repr(e)}', exc_info=True)

    def process_signal(self, signal: Signal):
        """
        :param signal: 信号
        :return: None
        """
        price = signal.price
        inst = signal.instrument
        if signal.type == SignalType.BUY:
            logger.info(f'{inst} 开多{signal.volume}手 价格: {price}')
            self.io_loop.create_task(self.buy(inst, price, signal.volume))
        elif signal.type == SignalType.SELL_SHORT:
            logger.info(f'{inst} 开空{signal.volume}手 价格: {price}')
            self.io_loop.create_task(self.sell_short(inst, price, signal.volume))
        elif signal.type == SignalType.BUY_COVER:
            pos = Trade.objects.filter(
                broker=self.__broker, strategy=self.__strategy, code=signal.code, instrument=inst, shares__gt=0,
                close_time__isnull=True, direction=DirectionType.values[DirectionType.SHORT]).first()
            logger.info(f'{pos.instrument} 平空{signal.volume}手 价格: {price}')
            self.io_loop.create_task(self.buy_cover(pos, price, signal.volume))
        elif signal.type == SignalType.SELL:
            pos = Trade.objects.filter(
                broker=self.__broker, strategy=self.__strategy, code=signal.code, instrument=inst, shares__gt=0,
                close_time__isnull=True, direction=DirectionType.values[DirectionType.LONG]).first()
            logger.info(f'{pos.instrument} 平多{signal.volume}手 价格: {price}')
            self.io_loop.create_task(self.sell(pos, price, signal.volume))
        elif signal.type == SignalType.ROLL_CLOSE:
            pos = Trade.objects.filter(
                broker=self.__broker, strategy=self.__strategy,
                code=signal.code, close_time__isnull=True, instrument=inst, shares__gt=0).first()
            if pos.direction == DirectionType.values[DirectionType.LONG]:
                logger.info(f'{pos.code}->{inst.main_code} 多头换月平旧{signal.volume}手 价格: {price}')
                self.io_loop.create_task(self.sell(pos, price, signal.volume))
            else:
                logger.info(f'{pos.code}->{inst.main_code} 空头换月平旧{signal.volume}手 价格: {price}')
                self.io_loop.create_task(self.buy_cover(pos, price, signal.volume))
        elif signal.type == SignalType.ROLL_OPEN:
            pos = Trade.objects.filter(
                Q(close_time__isnull=True) | Q(close_time__startswith=datetime.date.today()),
                broker=self.__broker, strategy=self.__strategy,
                shares=signal.volume, code=inst.last_main, instrument=inst, shares__gt=0).first()
            if pos.direction == DirectionType.values[DirectionType.LONG]:
                logger.info(f'{pos.code}->{inst.main_code} 多头换月开新{signal.volume}手 价格: {price}')
                self.io_loop.create_task(self.buy(inst, price, signal.volume))
            else:
                logger.info(f'{pos.code}->{inst.main_code} 空头换月开新{signal.volume}手 价格: {price}')
                self.io_loop.create_task(self.sell_short(inst, price, signal.volume))

    async def force_close_all(self) -> bool:
        try:
            logger.info('强制平仓..')
            for trade in Trade.objects.filter(close_time__isnull=True).all():
                shares = trade.filled_shares
                if trade.closed_shares:
                    shares -= trade.closed_shares
                bar = DailyBar.objects.filter(code=trade.code).order_by('-time').first()
                if trade.direction == DirectionType.values[DirectionType.LONG]:
                    await self.sell(trade, self.calc_up_limit(trade.instrument, bar), shares)
                else:
                    await self.buy_cover(trade, self.calc_down_limit(trade.instrument, bar), shares)
        except Exception as e:
            logger.warning(f'force_close_all 发生错误: {repr(e)}', exc_info=True)
            return False
        return True

    def calc_up_limit(self, inst: Instrument, bar: DailyBar):
        settlement = bar.settlement
        limit_ratio = str_to_number(self.raw_redis.get(f"LIMITRATIO:{inst.exchange}:{inst.product_code}:{bar.code}"))
        price_tick = inst.price_tick
        price = price_round(settlement * (Decimal(1) + Decimal(limit_ratio)), price_tick)
        return price - price_tick

    def calc_down_limit(self, inst: Instrument, bar: DailyBar):
        settlement = bar.settlement
        limit_ratio = str_to_number(self.raw_redis.get(f"LIMITRATIO:{inst.exchange}:{inst.product_code}:{bar.code}"))
        price_tick = inst.price_tick
        price = price_round(settlement * (Decimal(1) - Decimal(limit_ratio)), price_tick)
        return price + price_tick
