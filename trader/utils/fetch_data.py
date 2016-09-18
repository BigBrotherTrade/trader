import sys
import os
import datetime
import asyncio
import pytz

from tqdm import tqdm
import django

if sys.platform == 'darwin':
    sys.path.append('/Users/jeffchen/Documents/gitdir/dashboard')
else:
    sys.path.append('/home/cyh/bigbrother/dashboard')
os.environ["DJANGO_SETTINGS_MODULE"] = "dashboard.settings"
django.setup()
from trader.utils import is_trading_day, update_from_shfe, update_from_dce, update_from_czce, update_from_cffex, \
    create_main_all, fetch_from_quandl_all, clean_daily_bar, load_kt_data


async def fetch_bar():
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
        if trading:
            tasks += [
                asyncio.ensure_future(update_from_shfe(day)),
                asyncio.ensure_future(update_from_dce(day)),
                asyncio.ensure_future(update_from_czce(day)),
                asyncio.ensure_future(update_from_cffex(day)),
            ]
    print('task len=', len(tasks))
    for f in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
        await f


async def fetch_bar2():
    day = datetime.datetime.strptime('20100101', '%Y%m%d').replace(tzinfo=pytz.FixedOffset(480))
    end = datetime.datetime.strptime('20160118', '%Y%m%d').replace(tzinfo=pytz.FixedOffset(480))
    while day <= end:
        day, trading = await is_trading_day(day)
        if trading:
            print('process ', day)
            tasks = [
                asyncio.ensure_future(update_from_shfe(day)),
                asyncio.ensure_future(update_from_dce(day)),
                asyncio.ensure_future(update_from_czce(day)),
                asyncio.ensure_future(update_from_cffex(day)),
            ]
            await asyncio.wait(tasks)
        day += datetime.timedelta(days=1)
    print('all done!')


# asyncio.get_event_loop().run_until_complete(fetch_bar2())
# create_main_all()
# fetch_from_quandl_all()
# clean_dailybar()
load_kt_data()
