from typing import Union, Optional
from inspect import isclass

try:  # Assume we're a submodule in a package.
    from base.functions.arguments import get_name
    from base.classes.enum import DynamicEnum, EnumItem
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.functions.arguments import get_name
    from ...base.classes.enum import DynamicEnum, EnumItem


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
