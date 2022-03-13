from abc import ABC, abstractmethod
from typing import Optional, Iterable, Callable, Any

try:  # Assume we're a submodule in a package.
    from interfaces import (
        StructRowInterface, StructInterface, LoggerInterface, LoggingLevel,
        ItemType, Item, UniKey, FieldInterface, FieldName, FieldNo, Field, Name, Value, Class, Array,
        AUTO, Auto,
    )
    from base.functions.arguments import get_name, get_names
    from functions.primary import items as it
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        StructRowInterface, StructInterface, LoggerInterface, LoggingLevel,
        ItemType, Item, UniKey, FieldInterface, FieldName, FieldNo, Field, Name, Value, Class, Array,
        AUTO, Auto,
    )
    from ...base.functions.arguments import get_name, get_names
    from ...functions.primary import items as it


class AbstractDescription(ABC):
    def __init__(
            self,
            target_item_type: ItemType,
            input_item_type: ItemType = ItemType.Auto,
            skip_errors: bool = False,
            logger: Optional[LoggerInterface] = None,
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
    def get_input_fields(self, *args) -> Array:
        pass

    @abstractmethod
    def get_output_fields(self, *args) -> Array:
        pass

    def get_linked_fields(self) -> Array:
        return list(self.get_input_fields()) + list(self.get_output_fields())

    def get_input_field_names(self) -> list:
        return get_names(self.get_input_fields())

    def get_output_field_names(self, *args) -> list:
        return get_names(self.get_output_fields(*args))

    def get_target_field_name(self) -> Optional[str]:
        if hasattr(self, 'get_target_field'):
            target_field = self.get_target_field()
            if target_field:
                return get_name(target_field)

    @abstractmethod
    def get_output_field_types(self, struct: StructInterface) -> Iterable:
        pass

    def get_dict_output_field_types(self, struct: StructInterface) -> dict:
        names = self.get_output_field_names(struct)
        types = self.get_output_field_types(struct)
        return dict(zip(names, types))

    def has_data(self) -> bool:
        return bool(self.get_input_fields()) or bool(self.get_output_fields())

    def get_selection_tuple(self, including_target: bool = False) -> tuple:
        if including_target:
            return (self.get_target_field_name(), self.get_function(), *self.get_input_field_names())
        else:
            return (self.get_function(), *self.get_input_field_names())

    @abstractmethod
    def apply_inplace(self, item: Item) -> None:
        pass

    def __repr__(self):
        return str(self)

    def __str__(self):
        inputs = ', '.join(map(str, self.get_input_field_names()))
        outputs = ', '.join(map(str, self.get_output_field_names()))
        func = self.get_function().__name__
        return '{outputs}={func}({inputs})'.format(outputs=outputs, func=func, inputs=inputs)


class SingleFieldDescription(AbstractDescription, ABC):
    def __init__(
            self,
            field: Field,
            target_item_type: ItemType,
            input_item_type: ItemType = ItemType.Auto,
            skip_errors: bool = False,
            logger: Optional[LoggerInterface] = None,
            default: Any = None,
    ):
        super().__init__(
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger,
        )
        assert isinstance(field, (FieldName, FieldNo, FieldInterface)), 'got {} as {}'.format(field, type(field))
        self._target = field
        self._default = default

    def must_finish(self) -> bool:
        return False

    def get_default_value(self) -> Value:
        return self._default

    def get_target_field(self) -> Field:
        return self._target

    def get_output_fields(self, *args) -> list:
        return [self.get_target_field()]

    def get_input_fields(self) -> list:
        return list()

    def get_target_field_name(self) -> Name:
        return get_name(self.get_target_field())

    def get_input_values(self, item: Item) -> list:
        return it.get_fields_values_from_item(
            fields=self.get_input_field_names(),
            item=item, item_type=self.get_input_item_type(),
            skip_errors=self.must_skip_errors(), logger=self.get_logger(), default=self.get_default_value(),
        )

    def get_function(self) -> Callable:
        return lambda i: i

    def get_annotations(self) -> dict:
        function = self.get_function()
        if hasattr(function, '__annotations__'):
            return function.__annotations__
        else:
            return dict()

    def set_target_field(self, field: Field, inplace: bool):
        if inplace:
            self._target = field
            return self
        else:
            return self.make_new(field=field)

    def to(self, field: Field):
        if self.get_target_field() in ('_', AUTO, None):
            self.set_target_field(field, inplace=True)
            return self
        else:
            raise NotImplementedError

    @abstractmethod
    def get_value_from_item(self, item: Item) -> Value:
        pass

    def apply_inplace(self, item: Item) -> None:
        item_type = self.get_input_item_type()
        if item_type == AUTO:
            item_type = ItemType.detect(item, default=ItemType.Any)
        it.set_to_item_inplace(
            field=self.get_target_field_name(),
            value=self.get_value_from_item(item),
            item=item,
            item_type=item_type,
        )


class MultipleFieldDescription(AbstractDescription, ABC):
    def __init__(
            self,
            target_item_type: ItemType,
            input_item_type: ItemType = ItemType.Auto,
            skip_errors: bool = False,
            logger: Optional[LoggerInterface] = None,
    ):
        super().__init__(
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger,
        )


class TrivialMultipleDescription(MultipleFieldDescription, ABC):
    def __init__(
            self,
            target_item_type: ItemType,
            input_item_type: ItemType = ItemType.Auto,
            skip_errors: bool = False,
            logger: Optional[LoggerInterface] = None,
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

    def get_input_fields(self, struct: UniKey) -> Iterable:
        return self.get_output_fields(struct)

    def get_output_field_types(self, struct: UniKey) -> list:
        names = self.get_output_field_names(struct)
        types = [struct.get_field_description(f).get_type() for f in names]
        return types

    def apply_inplace(self, item: Item) -> None:
        pass
