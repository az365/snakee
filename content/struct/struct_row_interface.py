from abc import ABC, abstractmethod
from typing import Optional, Callable, Union

try:  # Assume we're a submodule in a package.
    from base.classes.auto import AUTO, Auto
    from base.interfaces.data_interface import SimpleDataInterface
    from content.fields.field_interface import FieldInterface
    from content.items.simple_items import SimpleRowInterface, SimpleRow, FieldNo, FieldID, Value
    from content.struct.struct_interface import StructInterface, StructMixinInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.auto import AUTO, Auto
    from ...base.interfaces.data_interface import SimpleDataInterface
    from ..fields.field_interface import FieldInterface
    from ..items.simple_items import SimpleRowInterface, SimpleRow, FieldNo, FieldID, Value
    from .struct_interface import StructInterface, StructMixinInterface

StructRow = SimpleDataInterface
GenericRow = Union[SimpleRow, StructRow]

DEFAULT_DELIMITER = '\t'


# @deprecated
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
    def get_values(self, fields: SimpleRow) -> SimpleRow:
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
    def set_value(
            self,
            field: FieldID,
            value: Value,
            field_type=AUTO,
            update_struct: bool = False,
            inplace: bool = True,
    ) -> Optional[StructRow]:
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
    def simple_select_fields(self, fields: SimpleRow) -> StructRow:
        pass
