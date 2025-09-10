import sys
import os
import django
if sys.platform == 'darwin':
    sys.path.append('/Users/jeffchen/Documents/gitdir/dashboard')
elif sys.platform == 'win32':
    sys.path.append(r'E:\github\dashboard')
else:
    sys.path.append('/root/gitee/dashboard')
os.environ["DJANGO_SETTINGS_MODULE"] = "dashboard.settings"
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()
import asyncio
import datetime
import pytz
from trader.utils import update_from_czce


if __name__ == "__main__":
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        day = datetime.datetime.now().replace(tzinfo=pytz.FixedOffset(480))
        day = day - datetime.timedelta(days=1)
        loop.run_until_complete(update_from_czce(day))
        print("DONE!")
    except KeyboardInterrupt:
        pass

