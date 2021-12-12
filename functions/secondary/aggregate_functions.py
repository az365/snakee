from typing import Optional, Callable, Iterable

try:  # Assume we're a sub-module in a package.
    from utils import (
        arguments as arg,
        mappers as ms,
    )
    from functions.primary import numeric as nm
    from functions.secondary import array_functions as af
    from functions.secondary import basic_functions as bf
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import (
        arguments as arg,
        mappers as ms,
    )
    from ..primary import numeric as nm
    from . import basic_functions as bf
    from . import array_functions as af


def sum() -> Callable:
    def func(array: Iterable) -> Optional[float]:
        return nm.sum(array)
    return func


def avg() -> Callable:
    def func(array: Iterable) -> Optional[float]:
        return nm.mean(array)
    return func


def median() -> Callable:
    def func(array: Iterable) -> Optional[float]:
        return nm.median(array)
    return func


def min() -> Callable:
    def func(array: Iterable) -> Optional[float]:
        return nm.min(array)
    return func


def max() -> Callable:
    def func(array: Iterable) -> Optional[float]:
        return nm.max(array)
    return func
