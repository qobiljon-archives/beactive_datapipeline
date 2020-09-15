from datetime import datetime
import time


def timestamp_to_datetime(ts):
    if ts:
        if ts > 13000000000:
            ts = ts / 1000
        return datetime.fromtimestamp(ts)
    else:
        return 0


def datetime_to_timestamp(dt):
    return time.mktime(dt.timetuple())


def date_str_to_timestamp(date: str, s: int):
    year = int(date[:4])
    month = date[4:6]
    day = date[6:8]
    dt = datetime(int(year), int(month), int(day))
    return datetime_to_timestamp(dt) + s


# get string YYYYMMDD from timestamp
def get_date_str_from_timestamp(ts):
    dt = timestamp_to_datetime(ts)
    return dt.strftime("%Y%m%d")
