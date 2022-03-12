from typing import Callable, Iterable, Union

try:  # Assume we're a submodule in a package.
    from base.functions.arguments import update
    from functions.primary import numeric as nm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.functions.arguments import update
    from ..primary import numeric as nm

OptFloat = nm.OptionalFloat


def sign(zero=0, plus=1, minus=-1) -> Callable:
    def func(value: float) -> int:
        return nm.sign(value, zero=zero, plus=plus, minus=minus)
    return func


def round_to(step: Union[int, float], exclude_negative: bool = False) -> Callable:
    return lambda v: nm.round_to(v, step=step, exclude_negative=exclude_negative)


def diff(
        constant: OptFloat = None,
        take_abs: bool = False,
        reverse: bool = False,
        default: OptFloat = None,
) -> Callable:
    def func(*args) -> float:
        if constant is None:
            assert len(args) == 2, 'Expected two values (constant={}), got {}'.format(constant, args)
            v, c = args
        else:
            assert len(args) == 1, 'Expected one value (constant={}), got {}'.format(constant, args)
            c = constant
            v = args[0]
        if reverse:
            c, v = v, c
        return nm.diff(v, c, take_abs=take_abs, default=default)
    return func


def increment(constant: OptFloat = None, take_abs: bool = False, default: OptFloat = None) -> Callable:
    return diff(constant, take_abs=take_abs, reverse=True, default=default)


def div(denominator: OptFloat = None, default: OptFloat = None) -> Callable:
    def func(x: float, y: OptFloat = None) -> OptFloat:
        if y is None:
            y = denominator
        elif denominator:
            msg = 'only one denominator allowed (from argument or from item), but both received: {} and {}'
            raise ValueError(msg.format(denominator, y))
        return nm.div(x, y, default=default)
    return func


def lift(
        baseline: OptFloat = None,
        take_abs: bool = False,
        reverse: bool = False,
        default: OptFloat = None,
) -> Callable:
    def func(x: OptFloat, y: OptFloat) -> OptFloat:
        if y is None:
            y = baseline
        elif baseline:
            msg = 'only one baseline allowed (from argument or from item), but both received: {} and {}'
            raise ValueError(msg.format(baseline, x if reverse else y))
        if reverse:
            x, y = y, x
        return nm.lift(x, y, take_abs=take_abs, default=default)
    return func


def mult(coefficient: OptFloat = None, default: OptFloat = None) -> Callable:
    def func(x: float, y: OptFloat = None) -> OptFloat:
        if y is None:
            y = coefficient
        elif coefficient:
            raise ValueError('only one coefficient allowed (from argument or from item), but both received')
        if x is None or y is None:
            return default
        else:
            return x * y
    return func


def sqrt(default: OptFloat = None) -> Callable:
    def func(value: float) -> OptFloat:
        return nm.sqrt(value=value, default=default)
    return func


def log(base: OptFloat, shift: float = 0, default: OptFloat = None) -> Callable:
    def func(value: float) -> OptFloat:
        return nm.log(value + shift, base=base, default=default)
    return func


def is_local_extreme(local_min=True, local_max=True) -> Callable:
    def func(*args) -> bool:
        args = update(args)
        assert len(args) == 3, 'is_local_extreme.func(): Expected 3 arguments, got {}'.format(args)
        return nm.is_local_extreme(*args, local_min=local_min, local_max=local_max)
    return func


def t_test_1sample_p_value(value: float = 0) -> Callable:
    def func(series: Iterable) -> float:
        return nm.t_test_1sample_p_value(series, value=value)
    return func


def p_log_sign(value: float = 0, default: float = -10.0) -> Callable:
    def func(series_or_p_value: Union[Iterable, float]) -> float:
        if isinstance(series_or_p_value, Iterable):
            p_value = nm.t_test_1sample_p_value(series_or_p_value)
        else:
            p_value = series_or_p_value
        p_log = nm.log(p_value, default=default)
        if p_log < default:
            p_log = default
        return p_log * nm.sign(value)
    return func
