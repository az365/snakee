from typing import Optional, Callable

try:  # Assume we're a sub-module in a package.
    from utils import numeric as nm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import numeric as nm


def sign() -> Callable:
    def func(value: float) -> int:
        if not value:
            return 0
        elif value > 0:
            return 1
        else:
            return -1
    return func


def diff(constant: Optional[float] = None, take_abs: bool = False, default: Optional[float] = None) -> Callable:
    def func(v: float, c: Optional[float] = None) -> float:
        if constant is not None:
            c = constant
        return nm.diff(c=c, v=v, take_abs=take_abs, default=default)
    return func


def div(denominator: Optional[float] = None, default: Optional[float] = None) -> Callable:
    def func(x: float, y: Optional[float] = None) -> Optional[float]:
        if y is None:
            y = denominator
        elif denominator:
            raise ValueError('only one denominator allowed (from argument or from item), but both received')
        return nm.div(x, y, default=default)
    return func


def mult(coefficient: Optional[float] = None, default: Optional[float] = None) -> Callable:
    def func(x: float, y: Optional[float] = None) -> Optional[float]:
        if y is None:
            y = coefficient
        elif coefficient:
            raise ValueError('only one coefficient allowed (from argument or from item), but both received')
        if x is None or y is None:
            return default
        else:
            return x * y
    return func


def sqrt(default: Optional[float] = None) -> Callable:
    def func(value: float) -> Optional[float]:
        return nm.sqrt(value=value, default=default)
    return func
