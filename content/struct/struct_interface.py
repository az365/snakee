from abc import ABC, abstractmethod
from typing import Optional, Union, Type

try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from connectors.databases.dialect_type import DialectType
    from content.fields.field_interface import FieldInterface
    from content.items.simple_items import FieldNo, FieldName, FieldID
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...connectors.databases.dialect_type import DialectType
    from ...content.fields.field_interface import FieldInterface
    from ..items.simple_items import FieldNo, FieldName, FieldID

Field = Union[FieldID, FieldInterface]
Array = Union[list, tuple]
ARRAY_SUBTYPES = list, tuple


class StructMixinInterface(ABC):
    @abstractmethod
    def get_columns(self) -> list:
        pass

    @abstractmethod
    def get_column_count(self) -> int:
        pass

    @abstractmethod
    def get_types_list(self, dialect: DialectType = DialectType.String) -> list:
        pass

    @abstractmethod
    def get_types_dict(self, dialect: DialectType = DialectType.String) -> dict:
        pass

    @abstractmethod
    def set_types(self, dict_field_types: Optional[dict] = None, inplace: bool = False, **kwargs):
        pass

    @abstractmethod
    def add_fields(self, *fields, default_type: Type = None, inplace: bool = False):
        pass

    @abstractmethod
    def remove_fields(self, *fields, inplace=True):
        pass

    @abstractmethod
    def get_field_position(self, field: FieldID) -> Optional[FieldNo]:
        pass

    @abstractmethod
    def get_fields_positions(self, names: Array):
        pass

    @abstractmethod
    def get_struct_str(self, dialect: DialectType = DialectType.Postgres) -> str:
        pass


Native = StructMixinInterface


class StructInterface(StructMixinInterface, ABC):
    @abstractmethod
    def append_field(self, field: Field, default_type=None, before=False, inplace=True) -> Optional[Native]:
        pass

    @abstractmethod
    def get_fields_count(self) -> int:
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
