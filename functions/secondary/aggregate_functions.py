from typing import Optional, Callable, Iterable

try:  # Assume we're a submodule in a package.
    from utils.decorators import sql_compatible
    from functions.primary import numeric as nm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.decorators import sql_compatible
    from ..primary import numeric as nm


@sql_compatible
def sum(_as_sql: bool = False) -> Callable:
    def func(*array) -> Optional[float]:
        if len(array) == 1:
            if isinstance(array[0], Iterable):
                array = array[0]
        return nm.sum(array)

    def get_sql_repr(*fields) -> str:
        if len(fields) == 1:
            return 'SUM({})'.format(fields[0])
        else:
            return ' + '.join(fields)

    return get_sql_repr if _as_sql else func


@sql_compatible
def avg(_as_sql: bool = False) -> Callable:
    def func(array: Iterable) -> Optional[float]:
        return nm.mean(array)

    def get_sql_repr(field: str) -> str:
        return 'MEAN({})'.format(field)

    return get_sql_repr if _as_sql else func


@sql_compatible
def median(continual: bool = True, _as_sql: bool = False) -> Callable:
    def func(array: Iterable) -> Optional[float]:
        return nm.median(array)

    def get_sql_repr(field: str) -> str:
        if continual:
            sql_func_name = 'PERCENTILE_CONT'
        else:
            sql_func_name = 'PERCENTILE_DESC'
        return '{}(0.5) WITHIN GROUP(ORDER BY {})'.format(sql_func_name, field)

    return get_sql_repr if _as_sql else func


@sql_compatible
def min(_as_sql: bool = False) -> Callable:
    def func(array: Iterable) -> Optional[float]:
        return nm.min(array)

    def get_sql_repr(field: str) -> str:
        return 'MIN({})'.format(field)

    return get_sql_repr if _as_sql else func


@sql_compatible
def max(_as_sql: bool = False) -> Callable:
    def func(array: Iterable, _as_sql: bool = False) -> Optional[float]:
        return nm.max(array)

    def get_sql_repr(field: str) -> str:
        return 'MAX({})'.format(field)

    return get_sql_repr if _as_sql else func
