from abc import ABC, abstractmethod
from typing import Optional, Any

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from fields.field_type import FieldType, FIELD_TYPES, DIALECTS, get_canonic_type
    from fields.field_interface import FieldInterface
    from base.abstract.simple_data import SimpleDataWrapper
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    # from .field_type import FieldType, EnumFieldType
    from .field_type import FieldType, FIELD_TYPES, DIALECTS, get_canonic_type
    from .field_interface import FieldInterface
    from ..base.abstract.simple_data import SimpleDataWrapper


class AbstractField(SimpleDataWrapper, FieldInterface, ABC):
    def __init__(self, name: str, field_type: FieldType = FieldType.Any, properties=None):
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
        elif hasattr(field_type, 'get_name'):
            return field_type.get_name()
        elif hasattr(field_type, 'value'):
            return str(field_type.value)
        else:
            return str(field_type)

    def get_type_in(self, dialect):
        if dialect is None:
            return self.get_type_name()
        else:
            assert dialect in DIALECTS
            return FIELD_TYPES.get(self.get_type_name(), {}).get(dialect)

    def __repr__(self):
        return '{}: {}'.format(self.get_name(), self.get_type_name())

    def __str__(self):
        return self.get_name()

    @abstractmethod
    def __add__(self, other):
        pass
