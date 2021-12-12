from typing import Callable

try:  # Assume we're a sub-module in a package.
    from functions.primary import numeric as nm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..primary import numeric as nm

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


def is_ordered(reverse: bool = False, including: bool = True) -> Callable:
    def func(previous, current) -> bool:
        if current == previous:
            return including
        elif reverse:
            return safe_more_than(current)(previous)
        else:
            return safe_more_than(previous)(current)
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
