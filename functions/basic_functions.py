from typing import Callable, Union

try:  # Assume we're a sub-module in a package.
    from utils import numeric as nm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import numeric as nm


DICT_CAST_TYPES = dict(bool=bool, int=int, float=float, str=str, text=str, date=str)
ZERO_VALUES = (None, 'None', '', '-', 0)


def partial(function, *args, **kwargs) -> Callable:
    def new_func(item):
        return function(item, *args, **kwargs)
    return new_func


def const(value) -> Callable:
    def func(_):
        return value
    return func


def defined() -> Callable:
    def func(value) -> bool:
        return value is not None
    return func


def is_none() -> Callable:
    def func(value) -> bool:
        return nm.is_none(value)
    return func


def not_none() -> Callable:
    def func(value) -> bool:
        return nm.is_defined(value)
    return func


def nonzero(zero_values=ZERO_VALUES) -> Callable:
    def func(value) -> bool:
        if nm.is_defined(value):
            return value not in zero_values
    return func


def equal(other) -> Callable:
    def func(value) -> bool:
        return value == other
    return func


def not_equal(other) -> Callable:
    def func(value) -> bool:
        return value != other
    return func


def less_than(other, including=False) -> Callable:
    def func(value) -> bool:
        if including:
            return value <= other
        else:
            return value < other
    return func


def more_than(other, including=False) -> Callable:
    def func(value) -> bool:
        if including:
            return value >= other
        else:
            return value > other
    return func


def at_least(number) -> Callable:
    return more_than(number, including=True)


def safe_more_than(other, including=False) -> Callable:
    def func(value) -> bool:
        first, second = value, other
        if type(first) != type(second):
            if not (isinstance(first, (int, float)) and isinstance(second, (int, float))):
                first = str(type(first))
                second = str(type(second))
        if including:
            return first >= second
        else:
            return first > second
    return func


def between(min_value, max_value, including=False) -> Callable:
    def func(value) -> bool:
        if including:
            return min_value <= value <= max_value
        else:
            return min_value < value < max_value
    return func


def not_between(min_value, max_value, including=False) -> Callable:
    func_between = between(min_value, max_value, including)

    def func(value) -> bool:
        return not func_between(value)
    return func


def apply_dict(dictionary, default=None) -> Callable:
    def func(key):
        return dictionary.get(key, default)
    return func


def cast(field_type, default_int=0) -> Callable:
    def func(value):
        cast_function = DICT_CAST_TYPES.get(field_type, field_type)
        if value in (None, 'None', '') and field_type in ('int', int, float):
            value = default_int
        return cast_function(value)
    return func


def percent(field_type=float, round_digits=1, default_value=None) -> Callable:
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
