from abc import ABC, abstractmethod
from typing import Optional, Union, Iterable, Callable, Any, NoReturn

try:  # Assume we're a sub-module in a package.
    from utils import (
        arguments as arg,
        items as it,
        selection as sf,
    )
    from items.item_type import ItemType
    from items.struct_row_interface import StructRowInterface
    from fields.abstract_field import AbstractField
    from items.struct_interface import StructInterface
    from loggers.logger_interface import LoggerInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import (
        arguments as arg,
        items as it,
        selection as sf,
    )
    from ..items.item_type import ItemType
    from ..items.struct_row_interface import StructRowInterface
    from ..fields.abstract_field import AbstractField
    from ..items.struct_interface import StructInterface
    from ..loggers.logger_interface import LoggerInterface

Row = Union[list, tuple]
Record = dict
Item = Union[StructRowInterface, Row, Record]
Name = Union[int, str]
Field = Union[Name, AbstractField]
Value = Any
Array = Union[list, tuple]
FieldList = Union[Array, StructInterface]
Logger = Optional[LoggerInterface]

AUTO = arg.AUTO
GIVE_SAME_FIELD_FOR_FUNCTION_DESCRIPTION = False


class AbstractDescription(ABC):
    def __init__(
            self,
            target_item_type: ItemType, input_item_type: ItemType = ItemType.Auto,
            skip_errors: bool = False, logger: Logger = None,
    ):
        self._target_type = target_item_type
        self._input_type = input_item_type
        self._skip_errors = skip_errors
        self._logger = logger

    def get_target_item_type(self) -> ItemType:
        return self._target_type

    def get_input_item_type(self) -> ItemType:
        return self._input_type

    def must_skip_errors(self) -> bool:
        return self._skip_errors

    def get_logger(self) -> LoggerInterface:
        return self._logger

    @abstractmethod
    def get_function(self) -> Callable:
        pass

    @abstractmethod
    def must_finish(self) -> bool:
        pass

    @abstractmethod
    def get_input_field_names(self, *args) -> Iterable:
        pass

    @abstractmethod
    def get_output_field_names(self, *args) -> Iterable:
        pass

    @abstractmethod
    def get_output_field_types(self, struct: StructInterface) -> Iterable:
        pass

    def get_dict_output_field_types(self, struct: StructInterface) -> dict:
        return dict(zip(self.get_output_field_names(), self.get_output_field_types(struct)))

    def get_selection_tuple(self) -> tuple:
        return (self.get_function(), *self.get_input_field_names())

    @abstractmethod
    def apply_inplace(self, item: Item) -> NoReturn:
        pass


class SingleFieldDescription(AbstractDescription, ABC):
    def __init__(
            self,
            field: Field,
            target_item_type: ItemType, input_item_type: ItemType = ItemType.Auto,
            skip_errors: bool = False, logger: Logger = None, default: Any = None,
    ):
        super().__init__(
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger,
        )
        assert isinstance(field, (int, str)), 'got {} as {}'.format(field, type(field))
        self._target = field
        self._default = default

    def must_finish(self) -> bool:
        return False

    def get_default_value(self) -> Value:
        return self._default

    def get_target_field_name(self) -> Name:
        return arg.get_name(self._target)

    def get_input_field_names(self) -> list:
        return list()

    def get_input_values(self, item: Item) -> list:
        return it.get_fields_values_from_item(
            fields=self.get_input_field_names(),
            item=item, item_type=self.get_input_item_type(),
            skip_errors=self.must_skip_errors(), logger=self.get_logger(), default=self.get_default_value(),
        )

    def get_output_field_names(self, *args) -> list:
        yield self.get_target_field_name()

    def get_function(self) -> Callable:
        return lambda i: i

    def get_annotations(self) -> dict:
        function = self.get_function()
        if hasattr(function, '__annotations__'):
            return function.__annotations__
        else:
            return dict()

    @abstractmethod
    def get_value_from_item(self, item) -> Value:
        pass

    def apply_inplace(self, item) -> NoReturn:
        item_type = self.get_input_item_type()
        if item_type == arg.AUTO:
            item_type = it.ItemType.detect(item, default=ItemType.Any)
        it.set_to_item_inplace(
            field=self.get_target_field_name(),
            value=self.get_value_from_item(item),
            item=item,
            item_type=item_type,
        )


class MultipleFieldDescription(AbstractDescription, ABC):
    def __init__(
            self,
            target_item_type: ItemType, input_item_type: ItemType = ItemType.Auto,
            skip_errors: bool = False, logger: Logger = None,
    ):
        super().__init__(
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger,
        )


class TrivialMultipleDescription(MultipleFieldDescription, ABC):
    def __init__(
            self,
            target_item_type: ItemType, input_item_type: ItemType = ItemType.Auto,
            skip_errors: bool = False, logger: Logger = None,
    ):
        super().__init__(
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger,
        )

    def must_finish(self) -> bool:
        return False

    @staticmethod
    def is_trivial_multiple() -> bool:
        return True

    def get_function(self) -> Callable:
        return lambda i: i

    def get_input_field_names(self, struct: FieldList) -> Iterable:
        return self.get_output_field_names(struct)

    def get_output_field_types(self, struct: FieldList) -> list:
        return [struct.get_field_description(f).get_type() for f in self.get_output_field_names()]

    def apply_inplace(self, item):
        pass
