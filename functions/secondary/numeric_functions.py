from typing import Optional, Callable, Union

try:  # Assume we're a submodule in a package.
    from functions.primary import numeric as nm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..primary import numeric as nm


def sign() -> Callable:
    def func(value: float) -> int:
        if not value:
            return 0
        elif value > 0:
            return 1
        else:
            return -1
    return func


def round_to(step: Union[int, float], exclude_negative: bool = False) -> Callable:
    return lambda v: nm.round_to(v, step=step, exclude_negative=exclude_negative)


def diff(constant: Optional[float] = None, take_abs: bool = False, default: Optional[float] = None) -> Callable:
    def func(*args) -> float:
        if constant is None:
            assert len(args) == 2, 'Expected two values (constant={}), got {}'.format(constant, args)
            c, v = args
        else:
            assert len(args) == 1, 'Expected one value (constant={}), got {}'.format(constant, args)
            c = constant
            v = args[0]
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
