from datetime import datetime, timedelta
from dateutil.parser import parse
import pytz
import re

start_UNIX_time = datetime(1970, 1, 1, tzinfo=pytz.UTC)
LOCAL_ZONE = pytz.timezone('Asia/Ho_Chi_Minh')

_TIME_PATTERN = re.compile(r"(\d+)([msdh])")

def from_str_time_to_milisecond(str_time, day_first=False):
    utc_time = from_str_to_utc(str_time, day_first)
    return from_datetime_to_milisecond(utc_time)


def from_str_to_utc(str_time, day_first=False):
    return parse(str_time, dayfirst=day_first).astimezone(pytz.UTC)


def from_datetime_to_milisecond(date_value):
    return int(((date_value - start_UNIX_time).total_seconds()) * 1000)


def from_milisecond_to_str_local(milisecond_value, time_str_format='%Y-%m-%d %H:%M:%S'):
    time_utc = from_milisecond_to_datetime(milisecond_value)
    time_local = time_utc.astimezone(LOCAL_ZONE)
    return time_local.strftime(time_str_format)


def from_milisecond_to_datetime(milisecond_value):
    return start_UNIX_time + timedelta(milliseconds=int(milisecond_value))


def make_time_ind(start_time, end_time, len_ind, interval):
    if len_ind <= 0:
        return []
    
    if len_ind == 1:
        return [from_milisecond_to_str_local(start_time)]
    
    interval_ms = convert_to_seconds(interval) * 1000
    total_ms = end_time - start_time
    
    if total_ms < interval_ms:
        return [from_milisecond_to_str_local(start_time)]
    
    if len_ind == 2:
        return [
            from_milisecond_to_str_local(start_time),
            from_milisecond_to_str_local(end_time)
        ]
    
    step_ms = total_ms / (len_ind - 1)
    times = []
    time_format = '%Y-%m-%d %H:%M:%S'
    
    for i in range(len_ind - 1):
        current_time_ms = start_time + int(i * step_ms)
        time_utc = from_milisecond_to_datetime(current_time_ms)
        time_local = time_utc.astimezone(LOCAL_ZONE)
        times.append(time_local.strftime(time_format))
    
    time_utc_end = from_milisecond_to_datetime(end_time)
    time_local_end = time_utc_end.astimezone(LOCAL_ZONE)
    times.append(time_local_end.strftime(time_format))
    
    return times

def convert_to_seconds(time_str):
    match = _TIME_PATTERN.match(time_str.strip())
    if not match:
        raise ValueError(f"Invalid time format: {time_str}")
    
    value = int(match.group(1))
    unit = match.group(2).lower()
    
    if unit == 'm':
        return value * 60
    elif unit == 'h':
        return value * 3600
    elif unit == 's':
        return value
    elif unit == 'd':
        return value * 86400
    else:
        raise ValueError(f"Unsupported time unit: {unit}")

