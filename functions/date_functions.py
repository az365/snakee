from typing import Callable

try:  # Assume we're a sub-module in a package.
    from utils import dates as dt
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import dates as dt


def date(as_iso_date: bool = True) -> Callable:
    def func(value):
        return dt.to_date(value, as_iso_date=as_iso_date)
    return func


def int_to_date(scale: str, as_iso_date: bool = True) -> Callable:
    if scale == 'day':
        return lambda d: dt.get_date_from_day_abs(d, as_iso_date=as_iso_date)
    elif scale == 'week':
        return lambda d: dt.get_date_from_week_abs(d, as_iso_date=as_iso_date)
    elif scale == 'month':
        return lambda d: dt.get_date_from_month_abs(d, as_iso_date=as_iso_date)
    else:
        raise ValueError(dt.ERR_TIME_SCALE.format(scale))


def date_to_int(scale: str) -> Callable:
    if scale == 'day':
        return dt.get_day_abs_from_date
    elif scale == 'week':
        return dt.get_week_abs_from_date
    elif scale == 'month':
        return dt.get_month_abs_from_date
    else:
        raise ValueError(dt.ERR_TIME_SCALE.format(scale))


def round_date(scale: str) -> Callable:
    if scale == 'day':
        return lambda d: d
    elif scale == 'week':
        return dt.get_monday_date
    elif scale == 'month':
        return dt.get_month_first_date
    else:
        raise ValueError(dt.ERR_TIME_SCALE.format(scale))


def next_date(scale: str, **kwargs) -> Callable:
    if scale == 'day':
        return lambda *args: dt.get_next_day_date(*args, **kwargs)
    elif scale == 'week':
        return lambda *args: dt.get_next_week_date(*args, **kwargs)
    elif scale == 'month':
        return lambda *args: dt.get_next_month_date(*args, **kwargs)
    elif scale == 'year':
        return lambda *args: dt.get_next_year_date(*args, **kwargs)
    else:
        raise ValueError(dt.ERR_TIME_SCALE.format(scale))


def date_range(scale: str, **kwargs) -> Callable:
    if scale == 'day':
        return lambda *args: dt.get_days_range(*args, **kwargs)
    elif scale == 'week':
        return lambda *args: dt.get_weeks_range(*args, **kwargs)
    elif scale == 'month':
        return lambda *args: dt.get_months_range(*args, **kwargs)
    else:
        raise ValueError(dt.ERR_TIME_SCALE.format(scale))
