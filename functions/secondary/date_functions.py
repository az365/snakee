from typing import Callable, Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from functions.primary import dates as dt
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ..primary import dates as dt

Scale = Union[dt.DateScale, str]


def date(as_iso_date: bool = True) -> Callable:
    def func(value):
        return dt.to_date(value, as_iso_date=as_iso_date)
    return func


def date_to_int(scale: Scale) -> Callable:
    return lambda d: dt.get_int_from_date(d, scale=scale)


def int_to_date(scale: Scale, as_iso_date: bool = True) -> Callable:
    return lambda d: dt.get_date_from_int(d, scale=scale, as_iso_date=as_iso_date)


def int_between(scale: Scale, rounded: bool = True, take_abs: bool = True) -> Callable:
    return lambda a, b: dt.get_int_between(a, b, scale=scale, rounded=rounded, take_abs=take_abs)


def round_date(scale: Scale, as_iso_date: Union[bool, arg.Auto] = arg.AUTO) -> Callable:
    return lambda d: dt.get_rounded_date(d, scale=scale, as_iso_date=as_iso_date)


def next_date(scale: Scale, **kwargs) -> Callable:
    return lambda d, *args: dt.get_next_date(d, scale=scale, *args, **kwargs)


def date_range(scale: Scale, **kwargs) -> Callable:
    return lambda d0, d1, *args: dt.get_dates_range(d0, d1, scale=scale, *args, **kwargs)
