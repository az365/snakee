from typing import Optional, Callable, Iterable, Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg, items as it, selection as sf
    from interfaces import ItemType, StructInterface, Item, Name, Field, Value, Array
    from selection.abstract_expression import SingleFieldDescription, TrivialMultipleDescription
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg, items as it, selection as sf
    from ..interfaces import ItemType, StructInterface, Item, Name, Field, Value, Array
    from .abstract_expression import SingleFieldDescription, TrivialMultipleDescription

GIVE_SAME_FIELD_FOR_FUNCTION_DESCRIPTION = False


class TrivialDescription(SingleFieldDescription):
    def __init__(
            self,
            field: Field,
            target_item_type: ItemType, input_item_type: ItemType = ItemType.Auto,
            skip_errors=False, logger=None, default=None,
    ):
        super().__init__(
            field=field,
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger, default=default,
        )

    def get_input_field_names(self) -> list:
        return [self.get_target_field_name()]

    def get_value_from_item(self, item: Item) -> Value:
        return it.get_field_value_from_item(
            field=self.get_target_field_name(),
            item=item, item_type=self.get_input_item_type(),
            skip_errors=self.must_skip_errors(), logger=self.get_logger(), default=self.get_default_value(),
        )

    def get_output_field_types(self, struct: StructInterface) -> list:
        field_name = self.get_target_field_name()
        return [struct.get_field_description(field_name).get_type()]


class AliasDescription(SingleFieldDescription):
    def __init__(
            self,
            alias: Field, source: Field,
            target_item_type: ItemType, input_item_type: ItemType = ItemType.Auto,
            skip_errors=False, logger=None, default=None,
    ):
        super().__init__(
            alias,
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger, default=default,
        )
        self._source_field = source

    def get_function(self) -> Callable:
        return lambda i: i

    def get_source_field(self) -> Field:
        return self._source_field

    def get_source_name(self) -> Name:
        return arg.get_name(self.get_source_field())

    def get_input_field_names(self) -> list:
        yield [self.get_source_name()]

    def get_value_from_item(self, item: Field) -> Value:
        return it.get_field_value_from_item(
            field=self.get_source_name(),
            item=item, item_type=self.get_input_item_type(),
            skip_errors=self.must_skip_errors(), logger=self.get_logger(), default=self.get_default_value(),
        )

    def get_output_field_types(self, struct) -> list:
        return [struct.get_field_description(f).get_type() for f in self.get_input_field_names()]


class RegularDescription(SingleFieldDescription):
    def __init__(
            self,
            target: Field, function: Callable, inputs: Array,
            target_item_type: ItemType, input_item_type: ItemType = ItemType.Auto,
            skip_errors=False, logger=None, default=None,
    ):
        super().__init__(
            target,
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger, default=default,
        )
        self._function = function
        self._inputs = inputs

    @classmethod
    def from_list(cls, target: Field, list_description, **kwargs) -> SingleFieldDescription:
        function, inputs = sf.process_description(list_description)
        return cls(target=target, function=function, inputs=inputs, **kwargs)

    def get_function(self) -> Callable:
        return self._function

    def get_return_type(self) -> Optional[type]:
        return self.get_annotations().get('return')

    def get_input_field_names(self) -> Iterable:
        return self._inputs

    def get_output_field_types(self, struct) -> list:
        return [self.get_return_type()]

    def get_value_from_item(self, item: Item) -> Value:
        return sf.safe_apply_function(
            function=self.get_function(),
            fields=self.get_input_field_names(),
            values=self.get_input_values(item),
            item=item,
            logger=self.get_logger(),
            skip_errors=self.must_skip_errors(),
        )


class FunctionDescription(SingleFieldDescription):
    def __init__(
            self,
            target: Field, function: Callable,
            give_same_field_to_input=GIVE_SAME_FIELD_FOR_FUNCTION_DESCRIPTION,
            target_item_type=ItemType.Auto, input_item_type=ItemType.Auto,
            skip_errors=False, logger=None, default=None,
    ):
        super().__init__(
            field=target,
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger, default=default,
        )
        self._function = function
        self.give_same_field_to_input = give_same_field_to_input

    def get_function(self) -> Callable:
        return self._function

    def get_return_type(self) -> Optional[type]:
        return self.get_annotations().get('return')

    def get_output_field_types(self, struct) -> list:
        return [self.get_return_type()]

    def get_input_field_names(self) -> list:
        if self.give_same_field_to_input:
            return [self.get_target_field_name()]
        else:
            return [it.STAR]

    def get_input_field_types(self) -> dict:
        field_types = self.get_annotations().copy()
        field_types.pop('return')
        return field_types

    def get_value_from_item(self, item) -> Value:
        return sf.safe_apply_function(
            function=self.get_function(),
            fields=self.get_input_field_names(),
            values=self.get_input_values(item),
            item=item,
            logger=self.get_logger(),
            skip_errors=self.must_skip_errors(),
        )


class StarDescription(TrivialMultipleDescription):
    def __init__(
            self,
            target_item_type: ItemType, input_item_type: ItemType = ItemType.Auto,
            skip_errors=False, logger=None,
    ):
        super().__init__(
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger,
        )

    def get_output_field_names(self, item_or_struct: Union[Item, StructInterface], item_type=arg.AUTO) -> Array:
        if isinstance(item_or_struct, StructInterface) or hasattr(item_or_struct, 'get_columns'):
            return item_or_struct.get_columns()
        else:  # isinstance(item_or_struct, Item)
            return it.get_fields_names_from_item(item_or_struct, item_type=item_type)

    def get_values_from_row(self, item: Item) -> Item:
        if self.get_target_item_type() == ItemType.Row:
            return item


class DropDescription(TrivialMultipleDescription):
    def __init__(
            self,
            drop_fields: Array,
            target_item_type: ItemType, input_item_type=ItemType.Auto,
            skip_errors=False, logger=None,
    ):
        super().__init__(
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger,
        )
        self._drop_fields = drop_fields

    def get_drop_fields(self) -> Array:
        return self._drop_fields

    def get_output_field_names(self, item_or_struct: Union[Item, StructInterface]) -> list:
        if isinstance(item_or_struct, StructInterface) or hasattr(item_or_struct, 'get_columns'):
            initial_fields = item_or_struct.get_columns()
        else:
            initial_fields = it.get_fields_names_from_item(item_or_struct, item_type=self.get_input_item_type())
        return [f for f in initial_fields if f not in self.get_drop_fields()]
