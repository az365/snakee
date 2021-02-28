import math
import numpy as np

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg


DICT_CAST_TYPES = dict(bool=bool, int=int, float=float, str=str, text=str, date=str)
ZERO_VALUES = (None, 'None', '', '-', 0)


def partial(function, *args, **kwargs):
    def new_func(item):
        return function(item, *args, **kwargs)
    return new_func


def same():
    def func(item):
        return item
    return func


def const(value):
    def func(_):
        return value
    return func


def defined():
    def func(value):
        return value is not None
    return func


def is_none():
    def func(value):
        return value is None or value is np.nan or math.isnan(value)
    return func


def not_none():
    def func(value):
        return not is_none()(value)
    return func


def nonzero(zero_values=ZERO_VALUES):
    def func(value):
        return value not in zero_values
    return func


def equal(other):
    def func(value):
        return value == other
    return func


def not_equal(other):
    def func(value):
        return value != other
    return func


def more_than(other, including=False):
    def func(value):
        if including:
            return value >= other
        else:
            return value > other
    return func


def at_least(number):
    return more_than(number, including=True)


def safe_more_than(other, including=False):
    def func(value):
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


def between(min_value, max_value, including=False):
    def func(value):
        if including:
            return min_value <= value <= max_value
        else:
            return min_value < value < max_value
    return func


def not_between(min_value, max_value, including=False):
    func_between = between(min_value, max_value, including)

    def func(value):
        return not func_between(value)
    return func


def apply_dict(dictionary, default=None):
    def func(key):
        return dictionary.get(key, default)
    return func


def cast(field_type, default_int=0):
    def func(value):
        cast_function = DICT_CAST_TYPES.get(field_type, field_type)
        if value in (None, 'None', '') and field_type in ('int', int, float):
            value = default_int
        return cast_function(value)
    return func


def percent(field_type=float, round_digits=1, default_value=None):
    def func(value):
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
