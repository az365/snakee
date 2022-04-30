from typing import Optional, Callable, Iterable, Union

try:  # Assume we're a submodule in a package.
    from base.functions.arguments import update
    from utils.decorators import sql_compatible
    from functions.primary import numeric as nm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.functions.arguments import update
    from ...utils.decorators import sql_compatible
    from ..primary import numeric as nm

OptFloat = nm.OptionalFloat


@sql_compatible
def sign(zero=0, plus=1, minus=-1, _as_sql: bool = True) -> Callable:
    def _sign(value: float) -> int:
        return nm.sign(value, zero=zero, plus=plus, minus=minus)

    def get_sql_repr(field) -> str:
        return f'{field} / ABS({field}'

    return get_sql_repr if _as_sql else _sign


@sql_compatible
def round(
        ndigits: Optional[int] = None,
        step: Union[int, OptFloat] = None,
        exclude_negative: bool = False,
        _as_sql: bool = True,
) -> Callable:
    if step:
        assert ndigits is None, 'Only one of arguments allowed: ndigits or step, got {} and {}'.format(ndigits, step)
        return round_to(step=step, exclude_negative=exclude_negative, _as_sql=_as_sql)
    elif _as_sql:
        def get_sql_repr(field) -> str:
            if step:
                return round_to(step=step, _as_sql=_as_sql)
            else:
                return f'ROUND({field}, {ndigits})'
        return get_sql_repr
    else:
        return lambda v: nm.round_py(v, ndigits, exclude_negative=exclude_negative)


@sql_compatible
def round_to(step: Union[int, float], exclude_negative: bool = False, _as_sql: bool = False) -> Callable:
    def _round_to(v: float) -> float:
        return nm.round_to(v, step=step, exclude_negative=exclude_negative)

    def get_sql_repr(field) -> str:
        return f'INT({field} / {step}) * {step}'

    return get_sql_repr if _as_sql else _round_to


@sql_compatible
def diff(
        constant: OptFloat = None,
        take_abs: bool = False,
        reverse: bool = False,
        default: OptFloat = None,
        _as_sql: bool = True,
) -> Callable:
    def _diff(*args) -> float:
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

    def get_sql_repr(*fields) -> str:
        if constant is None:
            assert len(fields) == 2, 'Expected two values (constant={}), got {}'.format(constant, args)
            v, c = fields
        else:
            assert len(fields) == 1, 'Expected one value (constant={}), got {}'.format(constant, args)
            c = constant
            v = fields[0]
        if reverse:
            c, v = v, c
        return f'{v} - {c}'

    return get_sql_repr if _as_sql else _diff


@sql_compatible
def increment(
        constant: OptFloat = None,
        take_abs: bool = False,
        default: OptFloat = None,
        _as_sql: bool = True,
) -> Callable:
    return diff(constant, take_abs=take_abs, reverse=True, default=default, _as_sql=_as_sql)


@sql_compatible
def div(denominator: OptFloat = None, default: OptFloat = None, _as_sql: bool = False) -> Callable:
    def _div(x: float, y: OptFloat = None) -> OptFloat:
        if y is None:
            y = denominator
        elif denominator:
            msg = 'only one denominator allowed (from argument or from item), but both received: {} and {}'
            raise ValueError(msg.format(denominator, y))
        return nm.div(x, y, default=default)

    def get_sql_repr(*fields) -> str:
        if len(fields) == 1:
            assert denominator
            return '{x} / {y}'.format(x=fields[0], y=denominator)
        elif len(fields) == 2:
            assert not denominator
            return '{x} / {y}'.format(x=fields[0], y=fields[1])
        else:
            raise ValueError('fs.equal() operates 1 or 2 fields, got {}'.format(fields))

    return get_sql_repr if _as_sql else _div


@sql_compatible
def lift(
        baseline: OptFloat = None,
        take_abs: bool = False,
        reverse: bool = False,
        default: OptFloat = None,
        _as_sql: bool = True,
) -> Callable:
    def _lift(x: OptFloat, y: OptFloat) -> OptFloat:
        if y is None:
            y = baseline
        elif baseline:
            msg = 'only one baseline allowed (from argument or from item), but both received: {} and {}'
            raise ValueError(msg.format(baseline, x if reverse else y))
        if reverse:
            x, y = y, x
        return nm.lift(x, y, take_abs=take_abs, default=default)

    def get_sql_repr(*fields) -> str:
        if len(fields) == 1:
            assert baseline
            x = fields[0]
            y = baseline
        elif len(fields) == 2:
            assert not baseline
            x = fields[0]
            y = fields[1]
        else:
            raise ValueError('fs.lift() operates 1 or 2 fields, got {}'.format(fields))
        return f'({y} - {x} / {x}'

    return get_sql_repr if _as_sql else _lift


@sql_compatible
def mult(coefficient: OptFloat = None, default: OptFloat = None, _as_sql: bool = True) -> Callable:
    def _mult(x: float, y: OptFloat = None) -> OptFloat:
        if y is None:
            y = coefficient
        elif coefficient:
            raise ValueError('only one coefficient allowed (from argument or from item), but both received')
        if x is None or y is None:
            return default
        else:
            return x * y

    def get_sql_repr(*fields) -> str:
        if len(fields) == 1:
            assert coefficient
            x = fields[0]
            y = coefficient
        elif len(fields) == 2:
            assert not coefficient
            x = fields[0]
            y = fields[1]
        else:
            raise ValueError('fs.mult() operates 1 or 2 fields, got {}'.format(fields))
        return f'({x} * {y}'

    return get_sql_repr if _as_sql else _mult


@sql_compatible
def sqrt(default: OptFloat = None, _as_sql: bool = True) -> Callable:
    def _sqrt(value: float) -> OptFloat:
        return nm.sqrt(value=value, default=default)

    def get_sql_repr(field) -> str:
        return f'SQRT({field})'

    return get_sql_repr if _as_sql else _sqrt


@sql_compatible
def log(base: OptFloat, shift: float = 0, default: OptFloat = None, _as_sql: bool = True) -> Callable:
    def _log(value: float) -> OptFloat:
        return nm.log(value + shift, base=base, default=default)

    def get_sql_repr(field) -> str:
        return f'LOG({field} + {shift})'

    return get_sql_repr if _as_sql else _log


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
        p_log = nm.log(p_value, base=10, default=default)
        if p_log < default:
            p_log = default
        return p_log * nm.sign(value)
    return func
