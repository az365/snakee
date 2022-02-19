from typing import Callable

try:  # Assume we're a submodule in a package.
    from utils.decorators import sql_compatible
    from functions.primary import text as tx
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.decorators import sql_compatible
    from ..primary import text as tx


@sql_compatible
def startswith(prefix: str, _as_sql: bool = False) -> Callable:
    def func(line: str) -> bool:
        return line.startswith(prefix)

    def get_sql_repr(field: str) -> str:
        return "{} LIKE '{}%'".format(field, prefix)

    return get_sql_repr if _as_sql else func


@sql_compatible
def endswith(suffix: str, _as_sql: bool = False) -> Callable:
    def func(line: str) -> bool:
        return line.endswith(suffix)

    def get_sql_repr(field: str) -> str:
        return "{} LIKE '%{}'".format(field, suffix)

    return get_sql_repr if _as_sql else func


@sql_compatible
def contains(suffix: str, _as_sql: bool = False) -> Callable:
    def func(line: str) -> bool:
        return suffix in line

    def get_sql_repr(field: str) -> str:
        return "{} LIKE '%{}%'".format(field, suffix)

    return get_sql_repr if _as_sql else func
