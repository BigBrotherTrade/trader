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
from functools import reduce
from itertools import combinations

import pytz
import aiohttp
from django.db.models import F, Q, Max, Min
import demjson
import redis
import quandl
from talib.abstract import ATR
from tqdm import tqdm

from panel.models import *
from trader.utils import ApiStruct
from trader.utils.read_config import config
# from trader.utils import logger as my_logger

# logger = my_logger.get_logger('trade.utils')

max_conn_shfe = asyncio.Semaphore(15)
max_conn_dce = asyncio.Semaphore(5)
max_conn_czce = asyncio.Semaphore(15)
max_conn_cffex = asyncio.Semaphore(15)
max_conn_sina = asyncio.Semaphore(15)
quandl.ApiConfig.api_key = config.get('QuantDL', 'api_key')
# cffex_ip = '183.195.155.138'    # www.cffex.com.cn
cffex_ip = 'www.cffex.com.cn'    # www.cffex.com.cn
# shfe_ip = '220.248.39.134'      # www.shfe.com.cn
shfe_ip = 'www.shfe.com.cn'      # www.shfe.com.cn
# czce_ip = '220.194.205.169'     # www.czce.com.cn
czce_ip = 'www.czce.com.cn'     # www.czce.com.cn
# dce_ip = '218.25.154.94'        # www.dce.com.cn
dce_ip = 'www.dce.com.cn'        # www.dce.com.cn


def str_to_number(s):
    try:
        if not isinstance(s, str):
            return s
        return int(s)
    except ValueError:
        return float(s)


def price_round(x: Decimal, base: Decimal):
    """
    根据最小精度取整，例如对于IF最小精度是0.2，那么 1.3 -> 1.2, 1.5 -> 1.4
    :param x: Decimal 待取整的数
    :param base: Decimal 最小精度
    :return: float 取整结果
    """
    precision = 0
    s = str(round(base, 3) % 1)
    s = s.rstrip('0').rstrip('.') if '.' in s else s
    p1, *p2 = s.split('.')
    if p2:
        precision = len(p2[0])
    return round(base * round(x / base), precision)


async def is_trading_day(day: datetime.datetime):
    s = redis.StrictRedis(
        host=config.get('REDIS', 'host', fallback='localhost'),
        db=config.getint('REDIS', 'db', fallback=1), decode_responses=True)
    return day, day.strftime('%Y%m%d') in (s.get('TradingDay'), s.get('LastTradingDay'))
    # async with aiohttp.ClientSession() as session:
    #     await max_conn_cffex.acquire()
    #     async with session.get(
    #             'http://{}/fzjy/mrhq/{}/index.xml'.format(cffex_ip, day.strftime('%Y%m/%d')),
    #             allow_redirects=False) as response:
    #         max_conn_cffex.release()
    #         return day, response.status != 302


def get_expire_date(inst_code: str, day: datetime.datetime):
    expire_date = int(re.findall('\d+', inst_code)[0])
    if expire_date < 1000:
        year_exact = math.floor(day.year % 100 / 10)
        if expire_date < 100 and day.year % 10 == 9:
            year_exact += 1
        expire_date += year_exact * 1000
    return expire_date


async def update_from_shfe(day: datetime.datetime):
    try:
        async with aiohttp.ClientSession() as session:
            day_str = day.strftime('%Y%m%d')
            await max_conn_shfe.acquire()
            async with session.get('http://{}/data/dailydata/kx/kx{}.dat'.format(shfe_ip, day_str)) as response:
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
    except Exception as e:
        print('update_from_shfe failed: %s' % e)


async def fetch_czce_page(session, url):
    try:
        await max_conn_czce.acquire()
        rst = None
        async with session.get(url) as response:
            if response.status == 200:
                rst = await response.text(encoding='gbk')
        max_conn_czce.release()
        return rst
    except Exception as e:
        print('fetch_czce_page failed: %s' % e)


async def update_from_czce(day: datetime.datetime):
    try:
        async with aiohttp.ClientSession() as session:
            day_str = day.strftime('%Y%m%d')
            rst = await fetch_czce_page(
                session, 'http://{}/portal/DFSStaticFiles/Future/{}/{}/FutureDataDaily.txt'.format(
                    czce_ip, day.year, day_str))
            if rst is None:
                rst = await fetch_czce_page(
                    session, 'http://{}/portal/exchange/{}/datadaily/{}.txt'.format(
                        czce_ip, day.year, day_str))
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
                close = inst_data[5].replace(',', '') if Decimal(inst_data[5].replace(',', '')) > 0.1 \
                    else inst_data[6].replace(',', '')
                DailyBar.objects.update_or_create(
                    code=inst_data[0],
                    exchange=ExchangeType.CZCE, time=day, defaults={
                        'expire_date': get_expire_date(inst_data[0], day),
                        'open': inst_data[2].replace(',', '') if Decimal(inst_data[2].replace(',', '')) > 0.1
                        else close,
                        'high': inst_data[3].replace(',', '') if Decimal(inst_data[3].replace(',', '')) > 0.1
                        else close,
                        'low': inst_data[4].replace(',', '') if Decimal(inst_data[4].replace(',', '')) > 0.1
                        else close,
                        'close': close,
                        'settlement': inst_data[6].replace(',', '') if Decimal(inst_data[6].replace(',', '')) > 0.1 else
                        inst_data[1].replace(',', ''),
                        'volume': inst_data[9].replace(',', ''),
                        'open_interest': inst_data[10].replace(',', '')})
    except Exception as e:
        print('update_from_czce failed: %s' % e)


async def update_from_dce(day: datetime.datetime):
    try:
        async with aiohttp.ClientSession() as session:
            await max_conn_dce.acquire()
            async with session.post('http://{}/publicweb/quotesdata/exportDayQuotesChData.html'.format(dce_ip), data={
                    'dayQuotes.variety': 'all', 'dayQuotes.trade_type': 0, 'exportFlag': 'txt'}) as response:
                rst = await response.text()
                max_conn_dce.release()
                for lines in rst.split('\r\n')[1:-3]:
                    if '小计' in lines or '品种' in lines:
                        continue
                    inst_data_raw = [x.strip() for x in lines.split('\t')]
                    inst_data = []
                    for cell in inst_data_raw:
                        if len(cell) > 0:
                            inst_data.append(cell)
                    """
    [0'商品名称', 1'交割月份', 2'开盘价', 3'最高价', 4'最低价', 5'收盘价', 6'前结算价', 7'结算价', 8'涨跌', 9'涨跌1', 10'成交量', 11'持仓量', 12'持仓量变化', 13'成交额']
    ['豆一', '1611', '3,760', '3,760', '3,760', '3,760', '3,860', '3,760', '-100', '-100', '2', '0', '0', '7.52']
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
    except Exception as e:
        print('update_from_dce failed: %s' % e)


async def update_from_cffex(day: datetime.datetime):
    try:
        async with aiohttp.ClientSession() as session:
            await max_conn_cffex.acquire()
            async with session.get('http://{}/fzjy/mrhq/{}/index.xml'.format(
                    cffex_ip, day.strftime('%Y%m/%d'))) as response:
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
    except Exception as e:
        print('update_from_cffex failed: %s' % e)


async def update_from_sina(day: datetime.datetime, inst: Instrument):
    try:
        async with aiohttp.ClientSession() as session:
            await max_conn_sina.acquire()
            async with session.get('http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php'
                                   '/Market_Center.getHQFuturesData?page=1&num=20&sort=position&asc=0&'
                                   'node={}&base=futures'.format(inst.sina_code)) as response:
                print('获取', inst)
                rst = await response.text()
                max_conn_sina.release()
                for inst_data in demjson.decode(rst):
                    if '连续' in inst_data['name']:
                        continue
                    DailyBar.objects.update_or_create(
                        code=inst_data['symbol'],
                        exchange=inst.exchange, time=day, defaults={
                            'expire_date': get_expire_date(inst_data['symbol'], day),
                            'open': inst_data['open'] if inst_data['open'] != '0' else inst_data['close'],
                            'high': inst_data['high'] if inst_data['high'] != '0' else inst_data['close'],
                            'low': inst_data['low'] if inst_data['low'] != '0' else inst_data['close'],
                            'close': inst_data['close'],
                            'settlement': inst_data['settlement'] if inst_data['settlement'] != '0' else
                            inst_data['prevsettlement'],
                            'volume': inst_data['volume'],
                            'open_interest': inst_data['position']})
    except Exception as e:
        print('update_from_sina failed: %s' % e)


def store_main_bar(bar: DailyBar):
    MainBar.objects.update_or_create(
        exchange=bar.exchange, product_code=re.findall('[A-Za-z]+', bar.code)[0], time=bar.time, defaults={
            'code': bar.code,
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
        expire_date = get_expire_date(inst.main_code, day)
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


def calc_sma(price, period):
    return reduce(lambda x, y: ((period - 1) * x + y) / period, price)


def calc_corr(day: datetime.datetime):
    price_dict = dict()
    begin_day = day.replace(year=day.year - 3)
    for code in Strategy.objects.get(name='大哥2.0').instruments.all().order_by('id').values_list(
            'product_code', flat=True):
        price_dict[code] = to_df(MainBar.objects.filter(
            time__gte=begin_day.date(),
            product_code=code).order_by('time').values_list('time', 'close'))
        price_dict[code].index = pd.DatetimeIndex(price_dict[code].time)
        price_dict[code]['price'] = price_dict[code].close.pct_change()
    return pd.DataFrame({k: v.price for k, v in price_dict.items()}).corr()


def nCr(n, r):
    f = math.factorial
    return f(n) / f(r) / f(n-r)


def find_best_score(n: int=20):
    """
    一秒钟算5个组合，要算100年..
    """
    corr_matrix = calc_corr(datetime.datetime.today())
    code_list = Strategy.objects.get(name='大哥2.0').instruments.all().order_by('id').values_list(
        'product_code', flat=True)
    result = list()
    for code_list in tqdm(combinations(code_list, n), total=nCr(code_list.count(), n)):
        score_df = pd.DataFrame([corr_matrix.ix[i, j] for i, j in combinations(code_list, 2)])
        score = (round((1 - (score_df.abs() ** 2).mean()[0]) * 100, 3) - 50) * 2
        result.append((score, ','.join(code_list)))
    result.sort(key=lambda tup: tup[0])
    print('得分最高: ', result[-3:])
    print('得分最低: ', result[:3])


def calc_history_signal(inst: Instrument, day: datetime.datetime, strategy: Strategy):
    his_break_n = strategy.param_set.get(code='BreakPeriod').int_value
    his_atr_n = strategy.param_set.get(code='AtrPeriod').int_value
    his_long_n = strategy.param_set.get(code='LongPeriod').int_value
    his_short_n = strategy.param_set.get(code='ShortPeriod').int_value
    his_stop_n = strategy.param_set.get(code='StopLoss').int_value
    df = to_df(MainBar.objects.filter(
        time__lte=day.date(),
        exchange=inst.exchange, product_code=inst.product_code).order_by('time').values_list(
        'time', 'open', 'high', 'low', 'close', 'settlement'))
    df.index = pd.DatetimeIndex(df.time)
    df['atr'] = ATR(df, timeperiod=his_atr_n)
    df['short_trend'] = df.close
    df['long_trend'] = df.close
    for idx in range(1, df.shape[0]):
        df.ix[idx, 'short_trend'] = (df.ix[idx-1, 'short_trend'] * (his_short_n - 1) +
                                     df.ix[idx, 'close']) / his_short_n
        df.ix[idx, 'long_trend'] = (df.ix[idx-1, 'long_trend'] * (his_long_n - 1) +
                                    df.ix[idx, 'close']) / his_long_n
    df['high_line'] = df.close.rolling(window=his_break_n).max()
    df['low_line'] = df.close.rolling(window=his_break_n).min()
    cur_pos = 0
    last_trade = None
    for cur_idx in range(his_break_n+1, df.shape[0]):
        idx = cur_idx - 1
        cur_date = df.index[cur_idx].to_pydatetime().replace(tzinfo=pytz.FixedOffset(480))
        prev_date = df.index[idx].to_pydatetime().replace(tzinfo=pytz.FixedOffset(480))
        if cur_pos == 0:
            if df.short_trend[idx] > df.long_trend[idx] and df.close[idx] > df.high_line[idx-1]:
                new_bar = MainBar.objects.filter(
                    exchange=inst.exchange, product_code=inst.product_code, time=cur_date).first()
                Signal.objects.create(
                    code=new_bar.code, trigger_value=df.atr[idx],
                    strategy=strategy, instrument=inst, type=SignalType.BUY, processed=True,
                    trigger_time=prev_date, price=new_bar.open, volume=1, priority=PriorityType.LOW)
                last_trade = Trade.objects.create(
                    broker=strategy.broker, strategy=strategy, instrument=inst,
                    code=new_bar.code, direction=DirectionType.LONG,
                    open_time=cur_date, shares=1, filled_shares=1, avg_entry_price=new_bar.open)
                cur_pos = cur_idx
            elif df.short_trend[idx] < df.long_trend[idx] and df.close[idx] < df.low_line[idx-1]:
                new_bar = MainBar.objects.filter(
                    exchange=inst.exchange, product_code=inst.product_code,
                    time=df.index[cur_idx].to_pydatetime().date()).first()
                Signal.objects.create(
                    code=new_bar.code, trigger_value=df.atr[idx],
                    strategy=strategy, instrument=inst, type=SignalType.SELL_SHORT, processed=True,
                    trigger_time=prev_date, price=new_bar.open, volume=1, priority=PriorityType.LOW)
                last_trade = Trade.objects.create(
                    broker=strategy.broker, strategy=strategy, instrument=inst,
                    code=new_bar.code, direction=DirectionType.SHORT,
                    open_time=cur_date, shares=1, filled_shares=1, avg_entry_price=new_bar.open)
                cur_pos = cur_idx * -1
        elif cur_pos > 0 and prev_date > last_trade.open_time:
            hh = float(MainBar.objects.filter(
                exchange=inst.exchange, product_code=inst.product_code,
                time__gte=last_trade.open_time,
                time__lt=prev_date).aggregate(Max('high'))['high__max'])
            if df.close[idx] <= hh - df.atr[cur_pos-1] * his_stop_n:
                new_bar = MainBar.objects.filter(
                    exchange=inst.exchange, product_code=inst.product_code,
                    time=df.index[cur_idx].to_pydatetime().date()).first()
                Signal.objects.create(
                    strategy=strategy, instrument=inst, type=SignalType.SELL, processed=True,
                    code=new_bar.code,
                    trigger_time=prev_date, price=new_bar.open, volume=1, priority=PriorityType.LOW)
                last_trade.avg_exit_price = new_bar.open
                last_trade.close_time = cur_date
                last_trade.closed_shares = 1
                last_trade.profit = (new_bar.open - last_trade.avg_entry_price) * inst.volume_multiple
                last_trade.save()
                cur_pos = 0
        elif cur_pos < 0 and prev_date > last_trade.open_time:
            ll = float(MainBar.objects.filter(
                exchange=inst.exchange, product_code=inst.product_code,
                time__gte=last_trade.open_time,
                time__lt=prev_date).aggregate(Min('low'))['low__min'])
            if df.close[idx] >= ll + df.atr[cur_pos * -1-1] * his_stop_n:
                new_bar = MainBar.objects.filter(
                    exchange=inst.exchange, product_code=inst.product_code,
                    time=df.index[cur_idx].to_pydatetime().date()).first()
                Signal.objects.create(
                    code=new_bar.code,
                    strategy=strategy, instrument=inst, type=SignalType.BUY_COVER, processed=True,
                    trigger_time=prev_date, price=new_bar.open, volume=1, priority=PriorityType.LOW)
                last_trade.avg_exit_price = new_bar.open
                last_trade.close_time = cur_date
                last_trade.closed_shares = 1
                last_trade.profit = (last_trade.avg_entry_price - new_bar.open) * inst.volume_multiple
                last_trade.save()
                cur_pos = 0
        if cur_pos != 0 and cur_date.date() == day.date():
            last_trade.avg_exit_price = df.open[cur_idx]
            last_trade.close_time = cur_date
            last_trade.closed_shares = 1
            if last_trade.direction == DirectionType.LONG:
                last_trade.profit = (last_trade.avg_entry_price - Decimal(df.open[cur_idx])) * \
                                    inst.volume_multiple
            else:
                last_trade.profit = (Decimal(df.open[cur_idx]) - last_trade.avg_entry_price) * \
                                    inst.volume_multiple
            last_trade.save()


def calc_his_all(day: datetime.datetime):
    strategy = Strategy.objects.get(name='大哥2.0')
    for inst in strategy.instruments.all():
        print('process', inst)
        last_day = Trade.objects.filter(instrument=inst, close_time__isnull=True).values_list(
            'open_time', flat=True).first()
        if last_day is None:
            last_day = datetime.datetime.combine(
                MainBar.objects.filter(product_code=inst.product_code, time__lte=day).order_by(
                    '-time').values_list('time', flat=True).first(),
                datetime.time.min.replace(tzinfo=pytz.FixedOffset(480)))
        calc_history_signal(inst, last_day, strategy)


def calc_his_up_limit(inst: Instrument, bar: DailyBar):
    ratio = inst.up_limit_ratio
    ratio = Decimal(round(ratio, 3))
    price = price_round(bar.settlement * (Decimal(1) + ratio), inst.price_tick)
    return price - inst.price_tick


def calc_his_down_limit(inst: Instrument, bar: DailyBar):
    ratio = inst.down_limit_ratio
    ratio = Decimal(round(ratio, 3))
    price = price_round(bar.settlement * (Decimal(1) - ratio), inst.price_tick)
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
                            exchange=KT_MARKET[market], product_code=code, code=main_code,
                            time='{}-{}-{}'.format(date[:4], date[4:6], date[6:8]),
                            open=oo, high=hh, low=ll, close=cc, settlement=se, open_interest=oi, volume=vo,
                            basis=None))
            MainBar.objects.bulk_create(insert_list)
            Instrument.objects.filter(product_code=code).update(
                last_main=last_main, main_code=cur_main, change_time=change_time)
