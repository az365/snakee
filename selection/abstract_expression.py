from abc import ABC, abstractmethod
from typing import Optional, Union, Iterable, Callable, Any, NoReturn

try:  # Assume we're a sub-module in a package.
    from utils import (
        arguments as arg,
        items as it,
        selection as sf,
    )
    from items.base_item_type import ItemType
    from fields.abstract_field import AbstractField
    from fields.schema_interface import SchemaInterface
    from loggers.logger_interface import LoggerInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import (
        arguments as arg,
        items as it,
        selection as sf,
    )
    from ..items.base_item_type import ItemType
    from ..fields.abstract_field import AbstractField
    from ..fields.schema_interface import SchemaInterface
    from ..loggers.logger_interface import LoggerInterface

Logger = Optional[LoggerInterface]

AUTO = arg.DEFAULT
GIVE_SAME_FIELD_FOR_FUNCTION_DESCRIPTION = False

FieldID = Union[int, str, AbstractField]
Array = Union[list, tuple]
FieldList = Union[Array, SchemaInterface]


class AbstractDescription(ABC):
    def __init__(
            self,
            target_item_type: ItemType, input_item_type: ItemType = ItemType.Auto,
            skip_errors: bool = False, logger: Logger = None,
    ):
        self.target_type = target_item_type
        self.input_type = input_item_type
        self.skip_errors = skip_errors
        self.logger = logger

    def get_target_item_type(self) -> ItemType:
        return self.target_type

    def get_input_item_type(self) -> ItemType:
        return self.input_type

    def get_logger(self) -> LoggerInterface:
        return self.logger

    @abstractmethod
    def get_function(self) -> Callable:
        pass

    @abstractmethod
    def must_finish(self) -> bool:
        pass

    @abstractmethod
    def get_input_field_names(self) -> Iterable:
        pass

    @abstractmethod
    def get_output_field_names(self, *args) -> Iterable:
        pass

    @abstractmethod
    def get_output_field_types(self, schema: SchemaInterface) -> Iterable:
        pass

    def get_dict_output_field_types(self, schema: SchemaInterface) -> dict:
        return dict(zip(self.get_output_field_names(), self.get_output_field_types()))

    @abstractmethod
    def apply_inplace(self, item):
        pass


class SingleFieldDescription(AbstractDescription, ABC):
    def __init__(
            self,
            field: FieldID,
            target_item_type: ItemType, input_item_type: ItemType = ItemType.Auto,
            skip_errors: bool = False, logger: Logger = None, default: Any = None,
    ):
        super().__init__(
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger,
        )
        assert isinstance(field, (int, str)), 'got {} as {}'.format(field, type(field))
        self.target = field
        self.default = default

    def must_finish(self) -> bool:
        return False

    def get_target_field_name(self) -> FieldID:
        return self.target

    def get_input_field_names(self) -> list:
        return []

    def get_input_values(self, item):
        return it.get_fields_values_from_item(
            fields=self.get_input_field_names(),
            item=item, item_type=self.get_input_item_type(),
            skip_errors=self.skip_errors, logger=self.logger, default=self.default,
        )

    def get_output_field_names(self, *args) -> list:
        yield self.get_target_field_name()

    def get_function(self) -> Callable:
        return lambda i: i

    @abstractmethod
    def get_value_from_item(self, item):
        pass

    def apply_inplace(self, item) -> NoReturn:
        item_type = self.get_input_item_type()
        if item_type == arg.DEFAULT:
            item_type = it.ItemType.detect(item)
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

    def get_input_field_names(self, item) -> Iterable:
        return self.get_output_field_names(item)

    def get_output_field_types(self, schema):
        return [schema.get_field_description(f).get_field_type() for f in self.get_output_field_names()]

    def apply_inplace(self, item):
        pass
