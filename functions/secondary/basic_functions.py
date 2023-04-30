from typing import Callable, Iterable, Union, Any

try:  # Assume we're a submodule in a package.
    from base.constants.text import ZERO_VALUES
    from utils.decorators import sql_compatible
    from functions.primary import numeric as nm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.constants.text import ZERO_VALUES
    from ...utils.decorators import sql_compatible
    from ..primary import numeric as nm


@sql_compatible
def same(_as_sql: bool = False) -> Callable:
    def _same(item):
        return item

    def get_sql_repr(field: str) -> str:
        return field

    return get_sql_repr if _as_sql else _same


def partial(function, *args, **kwargs) -> Callable:
    def _partial(item: Any) -> Any:
        return function(item, *args, **kwargs)
    return _partial


@sql_compatible
def const(value: Any, _as_sql: bool = False) -> Callable:
    def _const(*_) -> Any:
        return value

    def get_sql_repr(field) -> str:
        return '{value} as {field}'.format(value=repr(value), field=field)

    return get_sql_repr if _as_sql else _const


@sql_compatible
def defined(_as_sql: bool = False) -> Callable:
    def _defined(value: Any) -> bool:
        return value is not None

    def get_sql_repr(field) -> str:
        return f'{field} NOT NONE'

    return get_sql_repr if _as_sql else _defined


@sql_compatible
def is_none(_as_sql: bool = False) -> Callable:
    def _is_none(value: Any) -> bool:
        return nm.is_none(value)

    def get_sql_repr(field) -> str:
        return f'{field} IS NONE'

    return get_sql_repr if _as_sql else _is_none


@sql_compatible
def not_none(_as_sql: bool = False) -> Callable:
    def _not_none(value: Any) -> bool:
        return nm.is_defined(value)

    def get_sql_repr(field) -> str:
        return f'{field} NOT NONE'

    return get_sql_repr if _as_sql else _not_none


@sql_compatible
def nonzero(zero_values: Union[set, list, tuple] = ZERO_VALUES, _as_sql: bool = False) -> Callable:
    def _nonzero(value: Any) -> bool:
        if nm.is_defined(value):
            return value not in zero_values

    def get_sql_repr(field) -> str:
        return f"{field} != 0 AND {field} != ''"

    return get_sql_repr if _as_sql else _nonzero


@sql_compatible
def equal(*args, _as_sql: bool = False) -> Callable:
    if len(args) == 0:
        benchmark = None
        is_benchmark_defined = False
    elif len(args) == 1:
        is_benchmark_defined = True
        benchmark = args[0]
    else:
        raise ValueError('fs.equal() accepts 0 or 1 argument, got {}'.format(args))

    def _equal(*a) -> bool:
        if len(a) == 1 and is_benchmark_defined:
            value, other = a[0], benchmark
        elif len(a) == 2 and not is_benchmark_defined:
            value, other = a
        else:
            raise ValueError('fs.equal() accepts 1 benchmark and 1 value, got benchmark={}, value={}'.format(args, a))
        return value == other

    def get_sql_repr(*fields, _sign: str = '=') -> str:
        if len(fields) == 1:
            assert is_benchmark_defined
            return '{field} {sign} {benchmark}'.format(field=fields[0], sign=_sign, benchmark=repr(benchmark))
        elif len(fields) == 2:
            assert not is_benchmark_defined
            return '{field} {sign} {benchmark}'.format(field=fields[0], sign=_sign, benchmark=fields[1])
        else:
            raise ValueError('fs.equal() operates 1 or 2 fields, got {}'.format(fields))

    return get_sql_repr if _as_sql else _equal


@sql_compatible
def not_equal(*args, _as_sql: bool = False) -> Callable:
    _func = equal(*args, _as_sql=_as_sql)

    def _not_equal(*a) -> bool:
        return not _func(*a)

    def get_sql_repr(*fields) -> str:
        return _func(*fields, _sign='!=')

    return get_sql_repr if _as_sql else _not_equal


@sql_compatible
def less_than(other: Any, including: bool = False, _as_sql: bool = False) -> Callable:
    def _less_than(value: Any) -> bool:
        if including:
            return value <= other
        else:
            return value < other

    def get_sql_repr(*fields) -> str:
        _func = equal(other, _as_sql=True)
        if including:
            return _func(*fields, _sign='<=')
        else:
            return _func(*fields, _sign='<')

    return get_sql_repr if _as_sql else _less_than


@sql_compatible
def more_than(other: Any, including: bool = False, _as_sql: bool = False) -> Callable:
    def _more_than(value: Any) -> bool:
        if including:
            return value >= other
        else:
            return value > other

    def get_sql_repr(*fields) -> str:
        _func = equal(other, _as_sql=True)
        if including:
            return _func(*fields, _sign='>=')
        else:
            return _func(*fields, _sign='>')

    return get_sql_repr if _as_sql else _more_than


@sql_compatible
def at_least(number: Any, _as_sql: bool = False) -> Callable:
    return more_than(number, including=True, _as_sql=_as_sql)


def safe_more_than(other: Any, including: bool = False, _as_sql: bool = False) -> Callable:
    def _safe_more_than(value) -> bool:
        first, second = value, other
        if type(first) != type(second):
            first_is_numeric = isinstance(first, nm.NUMERIC_TYPES)
            second_is_numeric = isinstance(second, nm.NUMERIC_TYPES)
            if first_is_numeric:
                if second_is_numeric:
                    first = float(first)
                    second = float(second)
                else:  # second is not numeric
                    return True
            elif second_is_numeric:
                return False
            else:
                first = str(type(first))
                second = str(type(second))
        try:
            if including:
                return first >= second
            else:
                return first > second
        except TypeError as e:
            if isinstance(first, Iterable) and isinstance(second, Iterable):
                for f, s in zip(first, second):
                    if safe_more_than(s, including=including)(f):
                        return True
                return False
            else:
                raise TypeError('{}: {} vs {}'.format(e, first, second))

    return _safe_more_than


def is_ordered(reverse: bool = False, including: bool = True) -> Callable:
    def _is_ordered(previous, current) -> bool:
        if current == previous:
            return including
        elif reverse:
            return safe_more_than(current)(previous)
        else:
            return safe_more_than(previous)(current)
    return _is_ordered


@sql_compatible
def between(min_value, max_value, including=False, _as_sql: bool = False) -> Callable:
    def _between(value) -> bool:
        if including:
            return min_value <= value <= max_value
        else:
            return min_value < value < max_value

    def get_sql_repr(field) -> str:
        return f'{field} BETWEEN {min_value} AND {max_value}'

    return get_sql_repr if _as_sql else _between


@sql_compatible
def not_between(min_value, max_value, including=False, _as_sql: bool = False) -> Callable:
    func_between = between(min_value, max_value, including)

    def _not_between(value) -> bool:
        return not func_between(value)

    def get_sql_repr(field) -> str:
        return f'{field} NOT BETWEEN {min_value} AND {max_value}'

    return get_sql_repr if _as_sql else _not_between


def apply_dict(dictionary, default=None) -> Callable:
    def _apply_dict(key):
        return dictionary.get(key, default)
    return _apply_dict


def acquire(default=None, zero_values: Union[list, tuple] = ZERO_VALUES) -> Callable:
    def _acquire(*values):
        for v in values:
            if v not in zero_values:
                return v
        return default
    return _acquire
