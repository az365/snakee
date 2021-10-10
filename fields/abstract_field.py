from abc import ABC, abstractmethod
from typing import Optional, Callable

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from interfaces import FieldInterface, FieldType, DialectType
    from base.abstract.simple_data import SimpleDataWrapper
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..interfaces import FieldInterface, FieldType, DialectType
    from ..base.abstract.simple_data import SimpleDataWrapper


class AbstractField(SimpleDataWrapper, FieldInterface, ABC):
    def __init__(self, name: str, field_type: FieldType = FieldType.Any, properties=None):
        field_type = arg.acquire(field_type, FieldType.detect_by_name, field_name=name)
        field_type = FieldType.get_canonic_type(field_type)
        assert isinstance(field_type, FieldType)
        self._type = field_type
        super().__init__(name=name, data=properties)

    def set_type(self, field_type: FieldType, inplace: bool) -> Optional[FieldInterface]:
        if inplace:
            self._type = field_type
        else:
            return self.set_outplace(field_type=field_type)

    def get_type(self) -> FieldType:
        return self._type

    def get_type_name(self) -> str:
        type_name = arg.get_value(self.get_type())
        if not isinstance(type_name, str):
            type_name = arg.get_name(type_name)
        return str(type_name)

    def get_type_in(self, dialect: DialectType):
        if not isinstance(dialect, DialectType):
            dialect = DialectType.detect(dialect)
        if dialect == DialectType.String:
            return self.get_type_name()
        else:
            return self.get_type().get_type_in(dialect)

    def get_converter(self, source: DialectType, target: DialectType) -> Callable:
        return self.get_type().get_converter(source, target)

    def __repr__(self):
        return '{}: {}'.format(self.get_name(), self.get_type_name())

    def __str__(self):
        return self.get_name()

    @abstractmethod
    def __add__(self, other):
        pass
