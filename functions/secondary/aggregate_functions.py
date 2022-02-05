from typing import Optional, Callable, Iterable

try:  # Assume we're a submodule in a package.
    from functions.primary import numeric as nm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..primary import numeric as nm


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
