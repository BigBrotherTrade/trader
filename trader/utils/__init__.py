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
from decimal import Decimal
import datetime
import math
import re
import xml.etree.ElementTree as ET
import asyncio
import os

import pytz
from bs4 import BeautifulSoup
import aiohttp
from django.db.models import F, Q, Max, Min
from django.db import connection
import redis
import quandl
import numpy as np
import pandas as pd
import talib
from talib.abstract import ATR, SMA
from django.db.models.sql import EmptyResultSet
from tqdm import tqdm

from panel.models import *
from trader.utils import ApiStruct
from trader.utils.read_config import config

max_conn_shfe = asyncio.Semaphore(15)
max_conn_dce = asyncio.Semaphore(5)
max_conn_czce = asyncio.Semaphore(15)
max_conn_cffex = asyncio.Semaphore(15)
quandl.ApiConfig.api_key = config.get('QuantDL', 'api_key')

his_break_n = 0
his_atr_n = 0
his_long_n = 0
his_short_n = 0
his_stop_n = 0
his_risk = 0
his_current = 0


def str_to_number(s):
    try:
        if not isinstance(s, str):
            return s
        return int(s)
    except ValueError:
        return float(s)


def myround(x: Decimal, base: Decimal):
    prec = 0
    s = str(round(base, 3) % 1)
    s = s.rstrip('0').rstrip('.') if '.' in s else s
    p1, *p2 = s.split('.')
    if p2:
        prec = len(p2[0])
    return round(base * round(x / base), prec)


async def is_trading_day(day: datetime.datetime):
    """
    判断是否是交易日, 方法是从中金所获取今日的K线数据,判断http的返回码(如果出错会返回302重定向至404页面),
    因为开市前也可能返回302, 所以适合收市后(下午)使用
    :return: bool
    """
    s = redis.StrictRedis(
        host=config.get('REDIS', 'host', fallback='localhost'),
        db=config.getint('REDIS', 'db', fallback=1), decode_responses=True)
    if day.strftime('%Y%m%d') == s.get('TradingDay'):
        return day, True
    async with aiohttp.ClientSession() as session:
        await max_conn_cffex.acquire()
        async with session.get(
                'http://www.cffex.com.cn/fzjy/mrhq/{}/index.xml'.format(day.strftime('%Y%m/%d')),
                allow_redirects=False) as response:
            max_conn_cffex.release()
            return day, response.status != 302


def calc_expire_date(inst_code: str, day: datetime.datetime):
    expire_date = int(re.findall('\d+', inst_code)[0])
    if expire_date < 1000:
        year_exact = math.floor(day.year % 100 / 10)
        if expire_date < 100 and day.year % 10 == 9:
            year_exact += 1
        expire_date += year_exact * 1000
    return expire_date


async def update_from_shfe(day: datetime.datetime):
    async with aiohttp.ClientSession() as session:
        day_str = day.strftime('%Y%m%d')
        await max_conn_shfe.acquire()
        async with session.get('http://www.shfe.com.cn/data/dailydata/kx/kx{}.dat'.format(day_str)) as response:
            rst_json = await response.json()
            max_conn_shfe.release()
            for inst_data in rst_json['o_curinstrument']:
                """
    {'OPENINTERESTCHG': -11154, 'CLOSEPRICE': 36640, 'SETTLEMENTPRICE': 36770, 'OPENPRICE': 36990,
    'PRESETTLEMENTPRICE': 37080, 'ZD2_CHG': -310, 'DELIVERYMONTH': '1609', 'VOLUME': 51102,
    'PRODUCTSORTNO': 10, 'ZD1_CHG': -440, 'OPENINTEREST': 86824, 'ORDERNO': 0, 'PRODUCTNAME': '铜                  ',
    'LOWESTPRICE': 36630, 'PRODUCTID': 'cu_f    ', 'HIGHESTPRICE': 37000}
                """
                # error_data = inst_data
                if inst_data['DELIVERYMONTH'] == '小计' or inst_data['PRODUCTID'] == '总计':
                    continue
                if '_' not in inst_data['PRODUCTID']:
                    continue
                DailyBar.objects.update_or_create(
                    code=inst_data['PRODUCTID'].split('_')[0] + inst_data['DELIVERYMONTH'],
                    exchange=ExchangeType.SHFE, time=day, defaults={
                        'expire_date': inst_data['DELIVERYMONTH'],
                        'open': inst_data['OPENPRICE'] if inst_data['OPENPRICE'] else inst_data['CLOSEPRICE'],
                        'high': inst_data['HIGHESTPRICE'] if inst_data['HIGHESTPRICE'] else
                        inst_data['CLOSEPRICE'],
                        'low': inst_data['LOWESTPRICE'] if inst_data['LOWESTPRICE']
                        else inst_data['CLOSEPRICE'],
                        'close': inst_data['CLOSEPRICE'],
                        'settlement': inst_data['SETTLEMENTPRICE'] if inst_data['SETTLEMENTPRICE'] else
                        inst_data['PRESETTLEMENTPRICE'],
                        'volume': inst_data['VOLUME'] if inst_data['VOLUME'] else 0,
                        'open_interest': inst_data['OPENINTEREST'] if inst_data['OPENINTEREST'] else 0})


async def fetch_czce_page(session, url):
    await max_conn_czce.acquire()
    rst = None
    async with session.get(url) as response:
        if response.status == 200:
            rst = await response.text(encoding='gbk')
    max_conn_czce.release()
    return rst


async def update_from_czce(day: datetime.datetime):
    async with aiohttp.ClientSession() as session:
        day_str = day.strftime('%Y%m%d')
        rst = await fetch_czce_page(
            session, 'http://www.czce.com.cn/portal/DFSStaticFiles/Future/{}/{}/FutureDataDaily.txt'.format(
                day.year, day_str))
        if rst is None:
            rst = await fetch_czce_page(
                session, 'http://www.czce.com.cn/portal/exchange/{}/datadaily/{}.txt'.format(
                    day.year, day_str))
        for lines in rst.split('\r\n')[1:-3]:
            if '小计' in lines or '品种' in lines:
                continue
            inst_data = [x.strip() for x in lines.split('|' if '|' in lines else ',')]
            # error_data = inst_data
            """
[0'品种月份', 1'昨结算', 2'今开盘', 3'最高价', 4'最低价', 5'今收盘', 6'今结算', 7'涨跌1', 8'涨跌2', 9'成交量(手)', 10'空盘量', 11'增减量', 12'成交额(万元)', 13'交割结算价']
['CF601', '11,970.00', '11,970.00', '11,970.00', '11,800.00', '11,870.00', '11,905.00', '-100.00',
'-65.00', '13,826', '59,140', '-10,760', '82,305.24', '']
            """
            DailyBar.objects.update_or_create(
                code=inst_data[0],
                exchange=ExchangeType.CZCE, time=day, defaults={
                    'expire_date': calc_expire_date(inst_data[0], day),
                    'open': inst_data[2].replace(',', '') if Decimal(inst_data[2].replace(',', '')) > 0.1
                    else inst_data[5].replace(',', ''),
                    'high': inst_data[3].replace(',', '') if Decimal(inst_data[3].replace(',', '')) > 0.1
                    else inst_data[5].replace(',', ''),
                    'low': inst_data[4].replace(',', '') if Decimal(inst_data[4].replace(',', '')) > 0.1
                    else inst_data[5].replace(',', ''),
                    'close': inst_data[5].replace(',', ''),
                    'settlement': inst_data[6].replace(',', '') if Decimal(inst_data[6].replace(',', '')) > 0.1 else
                    inst_data[1].replace(',', ''),
                    'volume': inst_data[9].replace(',', ''),
                    'open_interest': inst_data[10].replace(',', '')})


async def update_from_dce(day: datetime.datetime):
    async with aiohttp.ClientSession() as session:
        day_str = day.strftime('%Y%m%d')
        await max_conn_dce.acquire()
        async with session.post('http://www.dce.com.cn/PublicWeb/MainServlet', data={
            'action': 'Pu00011_result', 'Pu00011_Input.trade_date': day_str, 'Pu00011_Input.variety': 'all',
            'Pu00011_Input.trade_type': 0}) as response:
            rst = await response.text()
            max_conn_dce.release()
            soup = BeautifulSoup(rst, 'lxml')
            for tr in soup.select("tr")[2:-4]:
                inst_data = list(tr.stripped_strings)
                # error_data = inst_data
                """
[0'商品名称', 1'交割月份', 2'开盘价', 3'最高价', 4'最低价', 5'收盘价', 6'前结算价', 7'结算价', 8'涨跌', 9'涨跌1', 10'成交量', 11'持仓量', 12'持仓量变化', 13'成交额']
['豆一', '1609', '3,699', '3,705', '3,634', '3,661', '3,714', '3,668', '-53', '-46', '5,746', '5,104', '-976', '21,077.13']
                """
                if '小计' in inst_data[0]:
                    continue
                DailyBar.objects.update_or_create(
                    code=DCE_NAME_CODE[inst_data[0]] + inst_data[1],
                    exchange=ExchangeType.DCE, time=day, defaults={
                        'expire_date': inst_data[1],
                        'open': inst_data[2].replace(',', '') if inst_data[2] != '-' else
                        inst_data[5].replace(',', ''),
                        'high': inst_data[3].replace(',', '') if inst_data[3] != '-' else
                        inst_data[5].replace(',', ''),
                        'low': inst_data[4].replace(',', '') if inst_data[4] != '-' else
                        inst_data[5].replace(',', ''),
                        'close': inst_data[5].replace(',', ''),
                        'settlement': inst_data[7].replace(',', '') if inst_data[7] != '-' else
                        inst_data[6].replace(',', ''),
                        'volume': inst_data[10].replace(',', ''),
                        'open_interest': inst_data[11].replace(',', '')})


async def update_from_cffex(day: datetime.datetime):
    async with aiohttp.ClientSession() as session:
        await max_conn_cffex.acquire()
        async with session.get('http://www.cffex.com.cn/fzjy/mrhq/{}/index.xml'.format(
                day.strftime('%Y%m/%d'))) as response:
            rst = await response.text()
            max_conn_cffex.release()
            tree = ET.fromstring(rst)
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
                # error_data = list(inst_data.itertext())
                DailyBar.objects.update_or_create(
                    code=inst_data.findtext('instrumentid').strip(),
                    exchange=ExchangeType.CFFEX, time=day, defaults={
                        'expire_date': inst_data.findtext('expiredate')[2:6],
                        'open': inst_data.findtext('openprice').replace(',', '') if inst_data.findtext(
                            'openprice') else inst_data.findtext('closeprice').replace(',', ''),
                        'high': inst_data.findtext('highestprice').replace(',', '') if inst_data.findtext(
                            'highestprice') else inst_data.findtext('closeprice').replace(',', ''),
                        'low': inst_data.findtext('lowestprice').replace(',', '') if inst_data.findtext(
                            'lowestprice') else inst_data.findtext('closeprice').replace(',', ''),
                        'close': inst_data.findtext('closeprice').replace(',', ''),
                        'settlement': inst_data.findtext('settlementprice').replace(',', '')
                        if inst_data.findtext('settlementprice') else
                        inst_data.findtext('presettlementprice').replace(',', ''),
                        'volume': inst_data.findtext('volume').replace(',', ''),
                        'open_interest': inst_data.findtext('openinterest').replace(',', '')})


def store_main_bar(bar: DailyBar):
    MainBar.objects.update_or_create(
        exchange=bar.exchange, product_code=re.findall('[A-Za-z]+', bar.code)[0], time=bar.time, defaults={
            'cur_code': bar.code,
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'settlement': bar.settlement,
            'volume': bar.volume,
            'open_interest': bar.open_interest})


def handle_rollover(inst: Instrument, new_bar: DailyBar):
    """
    换月处理, 基差=新合约收盘价-旧合约收盘价, 从今日起之前的所有连续合约的OHLC加上基差
    """
    product_code = re.findall('[A-Za-z]+', new_bar.code)[0]
    old_bar = DailyBar.objects.filter(exchange=inst.exchange, code=inst.last_main, time=new_bar.time).first()
    main_bar = MainBar.objects.get(
        exchange=inst.exchange, product_code=product_code, time=new_bar.time)
    if old_bar is None:
        old_close = new_bar.close
    else:
        old_close = old_bar.close
    basis = new_bar.close - old_close
    main_bar.basis = basis
    basis = float(basis)
    main_bar.save(update_fields=['basis'])
    MainBar.objects.filter(exchange=inst.exchange, product_code=product_code, time__lte=new_bar.time).update(
        open=F('open') + basis, high=F('high') + basis,
        low=F('low') + basis, close=F('close') + basis, settlement=F('settlement') + basis)


def calc_main_inst(inst: Instrument, day: datetime.datetime):
    """
    [["2016-07-18","2116.000","2212.000","2106.000","2146.000","34"],...]
    """
    updated = False
    if inst.main_code is not None:
        expire_date = calc_expire_date(inst.main_code, day)
    else:
        expire_date = day.strftime('%y%m')
    # 条件1: 成交量最大 & (成交量>1万 & 持仓量>1万 or 股指) = 主力合约
    if inst.exchange == ExchangeType.CFFEX:
        check_bar = DailyBar.objects.filter(
            exchange=inst.exchange, code__regex='^{}[0-9]+'.format(inst.product_code),
            expire_date__gte=expire_date,
            time=day.date()).order_by('-volume').first()
    else:
        check_bar = DailyBar.objects.filter(
            exchange=inst.exchange, code__regex='^{}[0-9]+'.format(inst.product_code),
            expire_date__gte=expire_date,
            time=day.date(), volume__gte=10000, open_interest__gte=10000).order_by('-volume').first()
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
                expire_date__gte=expire_date, time=day.date()).order_by(
                '-volume', '-open_interest', 'code').first()
        inst.main_code = check_bar.code
        inst.change_time = day
        inst.save(update_fields=['main_code', 'change_time'])
        store_main_bar(check_bar)
    # 主力合约发生变化, 做换月处理
    elif check_bar is not None and inst.main_code != check_bar.code and check_bar.code > inst.main_code:
        inst.last_main = inst.main_code
        inst.main_code = check_bar.code
        inst.change_time = day
        inst.save(update_fields=['last_main', 'main_code', 'change_time'])
        store_main_bar(check_bar)
        handle_rollover(inst, check_bar)
        updated = True
    else:
        bar = DailyBar.objects.filter(exchange=inst.exchange, code=inst.main_code, time=day).first()
        # 若当前主力合约当天成交量为0, 需要换下一个合约
        if bar is None or bar.volume == 0 or bar.open_interest == Decimal(0):
            check_bar = DailyBar.objects.filter(
                exchange=inst.exchange, code__regex='^{}[0-9]+'.format(inst.product_code),
                expire_date__gte=expire_date, time=day.date()).order_by(
                '-volume', '-open_interest').first()
            print('check_bar=', check_bar)
            if check_bar is None:
                _, trading = asyncio.get_event_loop().run_until_complete(is_trading_day(day))
                if not trading:
                    return inst.main_code, updated
            if bar is None or bar.code != check_bar.code:
                inst.last_main = inst.main_code
                inst.main_code = check_bar.code
                inst.change_time = day
                inst.save(update_fields=['last_main', 'main_code', 'change_time'])
                store_main_bar(check_bar)
                handle_rollover(inst, check_bar)
                updated = True
            else:
                store_main_bar(bar)
        else:
            store_main_bar(bar)
    return inst.main_code, updated


def create_main(inst: Instrument):
    print('processing ', inst.product_code)
    if inst.change_time is None:
        for day in DailyBar.objects.filter(
                exchange=inst.exchange, code__regex='^{}[0-9]+'.format(inst.product_code)).order_by(
                'time').values_list('time', flat=True).distinct():
            print(day, calc_main_inst(inst, datetime.datetime.combine(
                day, datetime.time.min.replace(tzinfo=pytz.FixedOffset(480)))))
    else:
        for day in DailyBar.objects.filter(
                time__gt=inst.change_time.date(),
                exchange=inst.exchange, code__regex='^{}[0-9]+'.format(inst.product_code)).order_by(
                'time').values_list('time', flat=True).distinct():
            print(day, calc_main_inst(inst, datetime.datetime.combine(
                day, datetime.time.min.replace(tzinfo=pytz.FixedOffset(480)))))


def create_main_all():
    for inst in Instrument.objects.filter(id__gte=16):
        create_main(inst)
    print('all done!')


def is_auction_time(inst: Instrument, status: dict):
    if status['InstrumentStatus'] == ApiStruct.IS_AuctionOrdering:
        now = datetime.datetime.now().replace(tzinfo=pytz.FixedOffset(480))
        if inst.exchange == ExchangeType.CFFEX:
            return True
        # 夜盘集合竞价时间是 20:55
        if inst.night_trade and now.hour == 20:
            return True
        # 日盘集合竞价时间是 8:55
        if not inst.night_trade and now.hour == 8:
            return True
    return False


def fetch_from_quandl(inst: Instrument):
    market = inst.exchange
    if inst.exchange == ExchangeType.CZCE:
        market = 'ZCE'
    prefix = '{}/{}'.format(market, inst.product_code.upper())
    for year in range(2010, 2017 + 1):
        print('year', year)
        for month, month_code in MONTH_CODE.items():
            quandl_code = prefix + month_code + str(year)
            print('month', month)
            rst = None
            try:
                rst = quandl.get(quandl_code)
            except quandl.QuandlError:
                pass
            if rst is None:
                continue
            rst.rename(columns={'O.I.': 'OI', 'Open Interest': 'OI', 'Prev. Day Open Interest': 'OI',
                                'Pre Settle': 'PreSettle'},
                       inplace=True)
            rst.Settle.fillna(rst.PreSettle, inplace=True)
            rst.Close.fillna(rst.Settle, inplace=True)
            rst.Open.fillna(rst.Close, inplace=True)
            rst.High.fillna(rst.Close, inplace=True)
            rst.Low.fillna(rst.Close, inplace=True)
            rst.OI.fillna(Decimal(0), inplace=True)
            rst.Volume.fillna(0, inplace=True)
            if inst.exchange == ExchangeType.CZCE:
                code = '{}{}{:02}'.format(inst.product_code, year % 10, month)
            else:
                code = '{}{}{:02}'.format(inst.product_code, year % 100, month)
            DailyBar.objects.bulk_create(
                DailyBar(
                    exchange=inst.exchange, code=code, time=row.Index.date(),
                    expire_date=int('{}{:02}'.format(year % 100, month)),
                    open=row.Open, high=row.High, low=row.Low, close=row.Close,
                    settlement=row.Settle, volume=row.Volume, open_interest=row.OI)
                for row in rst.itertuples())


def fetch_from_quandl_all():
    for inst in Instrument.objects.all():
        print('process', inst)
        fetch_from_quandl(inst)


def fetch_strategy_param(strategy: Strategy):
    global his_break_n
    global his_atr_n
    global his_long_n
    global his_short_n
    global his_stop_n
    global his_risk
    global his_current
    his_break_n = strategy.param_set.get(code='BreakPeriod').int_value
    his_atr_n = strategy.param_set.get(code='AtrPeriod').int_value
    his_long_n = strategy.param_set.get(code='LongPeriod').int_value
    his_short_n = strategy.param_set.get(code='ShortPeriod').int_value
    his_stop_n = strategy.param_set.get(code='StopLoss').int_value
    his_risk = strategy.param_set.get(code='Risk').float_value
    his_current = 10000000


def calc_history_signal(inst: Instrument, day: datetime.datetime, strategy: Strategy):
    try:
        df = to_df(MainBar.objects.filter(
            exchange=inst.exchange, product_code=inst.product_code).order_by('time').values_list(
            'time', 'open', 'high', 'low', 'close', 'settlement'))
        df.index = pd.DatetimeIndex(df.time)
        df['atr'] = ATR(df, timeperiod=his_atr_n)
        df['short_trend'] = SMA(df, timeperiod=his_short_n)
        df['long_trend'] = SMA(df, timeperiod=his_long_n)
        df['high_line'] = df.close.rolling(window=his_break_n).max()
        df['low_line'] = df.close.rolling(window=his_break_n).min()
        pos = 0
        pos_idx = None
        for idx in range(his_long_n - 1, df.shape[0]):
            if pos == 0:
                if df.short_trend[idx] > df.long_trend[idx] and df.close[idx] > df.high_line[idx-1]:
                    pass
                elif df.short_trend[idx] < df.long_trend[idx] and df.close[idx] < df.low_line[idx-1]:
                    pass
            elif pos > 0:

        # 查询该品种目前持有的仓位, 条件是开仓时间<=今天, 尚未未平仓或今天以后平仓(回测用)
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
                if close <= hh - atr_s[-open_count] * his_stop_n:
                    signal = SignalType.SELL
                    # 止损时 signal_value 为止损价
                    signal_value = hh - atr_s[-open_count] * his_stop_n
                    volume = pos.shares
                    last_bar = DailyBar.objects.filter(
                        exchange=inst.exchange, code=pos.code, time=day.date()).first()
                    price = calc_his_down_limit(inst, last_bar)
                # 多头换月
                elif roll_over:
                    signal = SignalType.ROLLOVER
                    volume = pos.shares
                    last_bar = DailyBar.objects.filter(
                        exchange=inst.exchange, code=pos.code, time=day.date()).first()
                    # 换月时 signal_value 为旧合约的平仓价
                    signal_value = calc_his_down_limit(inst, last_bar)
                    new_bar = DailyBar.objects.filter(
                        exchange=inst.exchange, code=inst.main_code, time=day.date()).first()
                    price = calc_his_up_limit(inst, new_bar)
            # 空头持仓
            else:
                ll = float(MainBar.objects.filter(
                    exchange=inst.exchange, product_code=pos.instrument.product_code,
                    time__gte=pos.open_time.date(), time__lte=day).aggregate(Min('low'))['low__min'])
                # 空头止损
                if close >= ll + atr_s[-open_count] * his_stop_n:
                    signal = SignalType.BUY_COVER
                    signal_value = ll + atr_s[-open_count] * his_stop_n
                    volume = pos.shares
                    last_bar = DailyBar.objects.filter(
                        exchange=inst.exchange, code=pos.code, time=day.date()).first()
                    price = calc_his_up_limit(inst, last_bar)
                # 空头换月
                elif roll_over:
                    signal = SignalType.ROLLOVER
                    volume = pos.shares
                    last_bar = DailyBar.objects.filter(
                        exchange=inst.exchange, code=pos.code, time=day.date()).first()
                    signal_value = calc_his_up_limit(inst, last_bar)
                    new_bar = DailyBar.objects.filter(
                        exchange=inst.exchange, code=inst.main_code, time=day.date()).first()
                    price = calc_his_down_limit(inst, new_bar)
        # 做多
        elif buy_sig:
            volume = his_current * his_risk // (Decimal(atr) * Decimal(inst.volume_multiple))
            if volume > 0:
                signal = SignalType.BUY
                signal_value = high_line
                new_bar = DailyBar.objects.filter(
                    exchange=inst.exchange, code=inst.main_code, time=day.date()).first()
                price = calc_his_up_limit(inst, new_bar)
        # 做空
        elif sell_sig:
            volume = his_current * his_risk // (Decimal(atr) * Decimal(inst.volume_multiple))
            if volume > 0:
                signal = SignalType.SELL_SHORT
                signal_value = low_line
                new_bar = DailyBar.objects.filter(
                    exchange=inst.exchange, code=inst.main_code, time=day.date()).first()
                price = calc_his_down_limit(inst, new_bar)
        if signal is not None:
            Signal.objects.update_or_create(
                strategy=strategy, instrument=inst, type=signal, trigger_time=day, defaults={
                    'price': price, 'volume': volume, 'trigger_value': signal_value,
                    'priority': PriorityType.Normal, 'processed': False})
    except Exception as e:
        print('calc_signal failed:', e)


def calc_his_up_limit(inst: Instrument, bar: DailyBar):
    ratio = inst.up_limit_ratio
    ratio = Decimal(round(ratio, 3))
    price = myround(bar.settlement * (Decimal(1) + ratio), inst.price_tick)
    return price - inst.price_tick


def calc_his_down_limit(inst: Instrument, bar: DailyBar):
    ratio = inst.down_limit_ratio
    ratio = Decimal(round(ratio, 3))
    price = myround(bar.settlement * (Decimal(1) - ratio), inst.price_tick)
    return price + inst.price_tick

async def clean_daily_bar():
    day = datetime.datetime.strptime('20100416', '%Y%m%d').replace(tzinfo=pytz.FixedOffset(480))
    end = datetime.datetime.strptime('20160118', '%Y%m%d').replace(tzinfo=pytz.FixedOffset(480))
    tasks = []
    while day <= end:
        tasks.append(is_trading_day(day))
        day += datetime.timedelta(days=1)
    trading_days = []
    for f in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
        rst = await f
        trading_days.append(rst)
    tasks.clear()
    for day, trading in trading_days:
        if not trading:
            DailyBar.objects.filter(time=day.date()).delete()
    print('done!')


def load_kt_data(directory: str = '/Users/jeffchen/kt_data/'):
    """
    20100104,4121.000,4131.000,4090.000,4098.000,284296.0,321838,a1009
    """
    for filename in os.listdir(directory):
        if filename.endswith(".txt"):
            code_str = filename.split('9', maxsplit=1)[0]
            market, code = code_str.split('_')
            print('process', code)
            cur_main = last_main = change_time = None
            insert_list = []
            with open(os.path.join(directory, filename)) as f:
                for line in f:
                    date, oo, hh, ll, cc, se, oi, vo, main_code = line.split(',')
                    main_code = main_code[:-1]
                    if cur_main != main_code:
                        if last_main != main_code:
                            last_main = cur_main
                        cur_main = main_code
                        change_time = '{}-{}-{}'.format(date[:4], date[4:6], date[6:8])
                    insert_list.append(
                        MainBar(
                            exchange=KT_MARKET[market], product_code=code, cur_code=main_code,
                            time='{}-{}-{}'.format(date[:4], date[4:6], date[6:8]),
                            open=oo, high=hh, low=ll, close=cc, settlement=se, open_interest=oi, volume=vo,
                            basis=None))
            MainBar.objects.bulk_create(insert_list)
            Instrument.objects.filter(product_code=code).update(
                last_main=last_main, main_code=cur_main, change_time=change_time)


def to_df(queryset):
    """
    :param queryset: django.db.models.query.QuerySet
    :return: pandas.core.frame.DataFrame
    """
    try:
        query, params = queryset.query.sql_with_params()
    except EmptyResultSet:
        # Occurs when Django tries to create an expression for a
        # query which will certainly be empty
        # e.g. Book.objects.filter(author__in=[])
        return pd.DataFrame()
    return pd.io.sql.read_sql_query(query, connection, params=params)
