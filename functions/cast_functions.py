from typing import Callable, Union

try:  # Assume we're a sub-module in a package.
    from utils import (
        numeric as nm,
        dates as dt,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import (
        numeric as nm,
        dates as dt,
    )

FieldType = Union[type, str]

DICT_CAST_TYPES = dict(bool=bool, int=int, float=float, str=str, text=str, date=str)


def cast(field_type: FieldType, default_int: int = 0) -> Callable:
    def func(value):
        cast_function = DICT_CAST_TYPES.get(field_type, field_type)
        if value in (None, 'None', '') and field_type in ('int', int, float):
            value = default_int
        return cast_function(value)
    return func


def date(as_iso_date: bool = True) -> Callable:
    def func(value):
        return dt.to_date(value, as_iso_date=as_iso_date)
    return func


def percent(field_type: FieldType = float, round_digits: int = 1, default_value=None) -> Callable:
    def func(value) -> Union[int, float, str]:
        if value is None:
            return default_value
        else:
            cast_function = DICT_CAST_TYPES.get(field_type, field_type)
            value = round(100 * value, round_digits)
            value = cast_function(value)
            if cast_function == str:
                value += '%'
            return value
    return func


def number(
        field_type: FieldType = float,
        round_digits: int = 2, show_plus: bool = True,
        default_value=None, default_suffix: str = '',
) -> Callable:
    cast_function = DICT_CAST_TYPES.get(field_type, field_type)

    def func(value: float) -> Union[str, int, float]:
        if value is None:
            return default_value
        elif value == 0:
            return cast_function(value)
        if abs(value) < 1:
            return percent(field_type)(value)
        else:
            digit_count = len(str(int(value)))
            value = round(value, round_digits - digit_count)
            if cast_function == str:
                for suffix in (default_suffix, 'K', 'M', 'G', 'T'):
                    if abs(value) < 1000:
                        int_value = int(value)
                        if value == int_value:
                            value = int_value
                        sign = '+' if value > 0 and show_plus else ''
                        str_value = '{}{}{}'.format(sign, value, suffix)
                        return str_value
                    value = value / 1000
            else:
                return cast_function(value)
    return func
