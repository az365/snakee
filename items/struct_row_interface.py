from abc import ABC, abstractmethod
from typing import Optional, Callable, Union

try:  # Assume we're a sub-module in a package.
    from base.interfaces.data_interface import SimpleDataInterface
    from items.struct_interface import StructInterface, StructMixinInterface
    from items.simple_items import SimpleRowInterface, SimpleRow, FieldNo, FieldName, FieldID, Value
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..base.interfaces.data_interface import SimpleDataInterface
    from .struct_interface import StructInterface, StructMixinInterface
    from .simple_items import SimpleRowInterface, SimpleRow, FieldNo, FieldName, FieldID, Value

StructRow = SimpleDataInterface
GenericRow = Union[SimpleRow, StructRow]

DEFAULT_DELIMITER = '\t'


class StructRowInterface(SimpleDataInterface, SimpleRowInterface, StructMixinInterface, ABC):
    @abstractmethod
    def get_struct(self) -> StructInterface:
        pass

    @abstractmethod
    def get_data(self) -> SimpleRow:
        pass

    @abstractmethod
    def set_data(self, row: SimpleRow, check: bool = True, inplace: bool = True) -> Optional[StructRow]:
        pass

    @abstractmethod
    def set_value(self, field: FieldID, value: Value, inplace: bool = True) -> Optional[StructRow]:
        pass

    @abstractmethod
    def get_record(self) -> dict:
        pass

    @abstractmethod
    def get_line(self, dialect='str', delimiter: str = DEFAULT_DELIMITER, need_quotes: bool = False) -> str:
        pass

    @abstractmethod
    def get_columns(self) -> SimpleRow:
        pass

    @abstractmethod
    def get_field_position(self, field: FieldID) -> Optional[FieldNo]:
        pass

    @abstractmethod
    def get_fields_positions(self, fields: SimpleRow) -> SimpleRow:
        pass

    @abstractmethod
    def get_value(
            self,
            field: Union[FieldID, Callable],
            skip_missing: bool = False,
            logger=None,
            default: Value = None,
    ) -> Value:
        pass

    @abstractmethod
    def get_values(self, fields: SimpleRow) -> SimpleRow:
        pass

    @abstractmethod
    def simple_select_fields(self, fields: SimpleRow) -> StructRow:
        pass
