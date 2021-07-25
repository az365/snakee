from abc import ABC, abstractmethod
from typing import Union, Optional

try:  # Assume we're a sub-module in a package.
    from fields.field_interface import FieldInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from fields.field_interface import FieldInterface

FieldName = str
FieldNo = int
FieldID = Union[FieldNo, FieldName]
Field = Union[FieldID, FieldInterface]
Array = Union[list, tuple]
ARRAY_SUBTYPES = list, tuple


class StructInterface(ABC):
    @abstractmethod
    def append_field(self, field: Field, default_type=None, before=False, inplace=True):
        pass

    @abstractmethod
    def add_fields(self, *fields, default_type=None, inplace=False):
        pass

    @abstractmethod
    def remove_fields(self, *fields, inplace: bool = True):
        pass

    @abstractmethod
    def get_fields_count(self):
        pass

    @abstractmethod
    def get_struct_str(self, dialect='py'):
        pass

    @abstractmethod
    def get_columns(self):
        pass

    @abstractmethod
    def get_types(self, dialect):
        pass

    @abstractmethod
    def set_types(self, dict_field_types=None, inplace=False, **kwargs):
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
    def get_field_description(self, field: FieldID) -> FieldInterface:
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

    @abstractmethod
    def __iter__(self):
        pass

    @abstractmethod
    def __add__(self, other):
        pass


SchemaInterface = StructInterface
