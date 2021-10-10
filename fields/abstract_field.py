from abc import ABC, abstractmethod
from typing import Optional, Callable
from enum import Enum
from inspect import isclass

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from interfaces import DialectType
    from fields.field_type import FieldType, FIELD_TYPES, get_canonic_type
    from fields.field_interface import FieldInterface
    from base.abstract.simple_data import SimpleDataWrapper
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..interfaces import DialectType
    from .field_type import FieldType, FIELD_TYPES, get_canonic_type
    from .field_interface import FieldInterface
    from ..base.abstract.simple_data import SimpleDataWrapper


class AbstractField(SimpleDataWrapper, FieldInterface, ABC):
    def __init__(self, name: str, field_type: FieldType = FieldType.Any, properties=None):
        field_type = arg.acquire(field_type, FieldType.detect_by_name, field_name=name)
        self._type = get_canonic_type(field_type)
        super().__init__(name=name, data=properties)

    def set_type(self, field_type: FieldType, inplace: bool) -> Optional[FieldInterface]:
        if inplace:
            self._type = field_type
        else:
            return self.set_outplace(field_type=field_type)

    def get_type(self) -> FieldType:
        return self._type

    def get_type_name(self) -> str:
        field_type = self.get_type()
        if isinstance(field_type, str):
            return field_type
        elif hasattr(field_type, 'get_value'):
            return field_type.get_value()
        elif hasattr(field_type, 'value'):
            return str(field_type.value)
        elif isclass(field_type) and not isinstance(field_type, Enum):
            return field_type.__name__
        else:
            return str(field_type)

    def get_type_in(self, dialect: DialectType):
        if not isinstance(dialect, DialectType):
            dialect = DialectType.detect(dialect)
        if dialect == DialectType.String:
            return self.get_type_name()
        else:
            return FIELD_TYPES.get(self.get_type_name(), {}).get(dialect)

    def get_converter(self, source, target) -> Callable:
        converter_name = '{}_to_{}'.format(source, target)
        return FIELD_TYPES.get(self.get_type_name(), {}).get(converter_name, str)

    def __repr__(self):
        return '{}: {}'.format(self.get_name(), self.get_type_name())

    def __str__(self):
        return self.get_name()

    @abstractmethod
    def __add__(self, other):
        pass
