from abc import ABC, abstractmethod
from typing import Union, Optional

try:  # Assume we're a sub-module in a package.
    from base.interfaces.data_interface import SimpleDataInterface
    from items.struct_interface import StructInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..base.interfaces.data_interface import SimpleDataInterface
    from .struct_interface import StructInterface

SimpleRow = Union[list, tuple]
StructRow = SimpleDataInterface
FieldName = str
FieldNo = int
FieldId = Union[FieldName, FieldNo]


class StructRowInterface(SimpleDataInterface, ABC):
    @abstractmethod
    def get_struct(self) -> StructInterface:
        pass

    @abstractmethod
    def get_data(self) -> SimpleRow:
        pass

    @abstractmethod
    def set_data(self, row: SimpleRow, check: bool = True, inplace: bool = True) -> StructRow:
        pass

    @abstractmethod
    def set_value(self, field: FieldId, value):
        pass

    @abstractmethod
    def get_record(self) -> dict:
        pass

    @abstractmethod
    def get_line(self, dialect='str', delimiter: str = '\t', need_quotes: bool = False) -> str:
        pass

    @abstractmethod
    def get_columns(self) -> SimpleRow:
        pass

    @abstractmethod
    def get_field_position(self, field: FieldId) -> Optional[FieldNo]:
        pass

    @abstractmethod
    def get_fields_positions(self, fields: SimpleRow) -> SimpleRow:
        pass

    @abstractmethod
    def get_value(self, field: FieldId, skip_missing: bool = False, logger=None, default=None):
        pass

    @abstractmethod
    def get_values(self, fields: SimpleRow) -> SimpleRow:
        pass

    @abstractmethod
    def simple_select_fields(self, fields: SimpleRow) -> StructRow:
        pass
