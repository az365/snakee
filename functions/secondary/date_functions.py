from typing import Callable, Union

try:  # Assume we're a sub-module in a package.
    from functions.primary import dates as dt
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..primary import dates as dt

Scale = Union[dt.DateScale, str]


def date(as_iso_date: bool = True) -> Callable:
    def func(value):
        return dt.to_date(value, as_iso_date=as_iso_date)
    return func


def int_to_date(scale: Scale, as_iso_date: bool = True) -> Callable:
    return lambda d: dt.get_date_from_int(d, scale=scale, as_iso_date=as_iso_date)


def date_to_int(scale: Scale) -> Callable:
    scale = dt.DateScale.convert(scale)
    if scale == dt.DateScale.Day:
        return dt.get_day_abs_from_date
    elif scale == dt.DateScale.Week:
        return dt.get_week_abs_from_date
    elif scale == dt.DateScale.Month:
        return dt.get_month_abs_from_date
    elif scale == dt.DateScale.Year:
        return dt.get_year_abs_from_date
    else:
        raise ValueError(dt.DateScale.get_err_msg(scale))


def round_date(scale: Scale) -> Callable:
    scale = dt.DateScale.convert(scale)
    if scale == dt.DateScale.Day:
        return lambda d: d
    elif scale == dt.DateScale.Week:
        return dt.get_monday_date
    elif scale == dt.DateScale.Month:
        return dt.get_month_first_date
    elif scale == dt.DateScale.Year:
        return dt.get_year_first_date
    else:
        raise ValueError(dt.DateScale.get_err_msg(scale))


def next_date(scale: str, **kwargs) -> Callable:
    scale = dt.DateScale.convert(scale)
    if scale == dt.DateScale.Day:
        return lambda *args: dt.get_next_day_date(*args, **kwargs)
    elif scale == dt.DateScale.Week:
        return lambda *args: dt.get_next_week_date(*args, **kwargs)
    elif scale == dt.DateScale.Month:
        return lambda *args: dt.get_next_month_date(*args, **kwargs)
    elif scale == dt.DateScale.Year:
        return lambda *args: dt.get_next_year_date(*args, **kwargs)
    else:
        raise ValueError(dt.DateScale.get_err_msg(scale))


def date_range(scale: str, **kwargs) -> Callable:
    scale = dt.DateScale.convert(scale)
    if scale == dt.DateScale.Day:
        return lambda *args: dt.get_days_range(*args, **kwargs)
    elif scale == dt.DateScale.Week:
        return lambda *args: dt.get_weeks_range(*args, **kwargs)
    elif scale == dt.DateScale.Month:
        return lambda *args: dt.get_months_range(*args, **kwargs)
    else:
        raise ValueError(dt.DateScale.get_err_msg(scale))
