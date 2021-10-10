from typing import Union, Optional
from inspect import isclass

try:  # Assume we're a sub-module in a package.
    from utils.arguments import get_name
    from utils.enum import DynamicEnum, EnumItem
    from utils.decorators import deprecated, deprecated_with_alternative
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.arguments import get_name
    from ...utils.enum import DynamicEnum, EnumItem
    from ...utils.decorators import deprecated, deprecated_with_alternative

DIALECTS = ('str', 'py', 'pg', 'ch')


class DialectType(DynamicEnum):
    String = 'str'
    Python = 'py'
    Postgres = 'pg'
    Clickhouse = 'ch'

    @classmethod
    def detect(cls, obj, default: Union[Optional[DynamicEnum], str] = 'str') -> EnumItem:
        if isinstance(obj, DialectType):
            return obj
        if isinstance(obj, str):
            name = obj
        elif isclass(obj):
            name = obj.__name__
        else:
            name = get_name(obj)
        dialect_type = DialectType.find_instance(name)
        if not dialect_type:
            if 'Postgres' in name:
                dialect_type = DialectType.Postgres
            elif 'Click' in name:
                dialect_type = DialectType.Clickhouse
            else:
                dialect_type = default
            if not isinstance(dialect_type, DialectType):
                dialect_type = DialectType.find_instance(dialect_type)
        return dialect_type


DialectType.prepare()


@deprecated
def get_dialect_type_from_conn_type_name(
        conn_type: Union[DynamicEnum, str],
        default: DialectType = DialectType.Python,
        other: DialectType = DialectType.String,
) -> DialectType:
    if conn_type is None:
        dialect_type = default
    else:
        conn_name = get_name(conn_type)
        if 'Postgres' in conn_name:
            dialect_type = DialectType.Postgres
        elif 'Click' in conn_name:
            dialect_type = DialectType.Clickhouse
        else:
            dialect_type = other
    if not isinstance(dialect_type, DialectType):
        dialect_type = DialectType.find_instance(dialect_type)
    return dialect_type


@deprecated_with_alternative('DialectType.detect()')
def get_dialect_for_connector(connector) -> DialectType:
    dialect_type = DialectType.detect(connector)
    assert isinstance(dialect_type, DialectType)
    return dialect_type
