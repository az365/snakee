from abc import ABC, abstractmethod
from typing import Union, Optional

try:  # Assume we're a sub-module in a package.
    from connectors.databases import dialect as di
    from fields import field_type as ft
    from fields.abstract_field import AbstractField
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..connectors.databases import dialect as di
    from ..fields import field_type as ft
    from ..fields.abstract_field import AbstractField

FieldName = str
FieldNo = int
FieldID = Union[FieldNo, FieldName]
Field = Union[FieldID, AbstractField]
Array = Union[list, tuple]
ARRAY_SUBTYPES = list, tuple


class SchemaInterface(ABC):
    @abstractmethod
    def append_field(self, field: Field, default_type=None, before=False):
        pass

    @abstractmethod
    def add_fields(self, *fields, default_type=None, return_schema=True):
        pass

    @abstractmethod
    def get_fields_count(self):
        pass

    @abstractmethod
    def get_schema_str(self, dialect='py'):
        pass

    @abstractmethod
    def get_columns(self):
        pass

    @abstractmethod
    def get_types(self, dialect):
        pass

    @abstractmethod
    def set_types(self, dict_field_types=None, return_schema=True, **kwargs):
        pass

    @abstractmethod
    def get_field_position(self, field: FieldID) -> Optional[FieldNo]:
        pass

    @abstractmethod
    def get_fields_positions(self, names: Array):
        pass

    @abstractmethod
    def get_converters(self, src='str', dst='py'):
        pass

    @abstractmethod
    def get_field_description(self, field_name):
        pass

    @abstractmethod
    def get_fields_descriptions(self) -> list:
        pass

    @abstractmethod
    def is_valid_row(self, row) -> bool:
        pass

    @abstractmethod
    def copy(self):
        pass

    @abstractmethod
    def simple_select_fields(self, fields: Array):
        pass
