from typing import Optional, Callable, Union

try:  # Assume we're a submodule in a package.
    from interfaces import ItemType, StructInterface, Item, Name, Field, Value, LoggerInterface, Array
    from base.functions.arguments import get_name
    from functions.primary.items import ItemType, get_field_value_from_item, get_fields_names_from_item, ALL
    from content.selection.selection_functions import process_description, safe_apply_function
    from content.selection.abstract_expression import SingleFieldDescription, TrivialMultipleDescription, Struct
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import ItemType, StructInterface, Item, Name, Field, Value, LoggerInterface, Array
    from ...base.functions.arguments import get_name
    from ...functions.primary.items import ItemType, get_field_value_from_item, get_fields_names_from_item, ALL
    from .selection_functions import process_description, safe_apply_function
    from .abstract_expression import SingleFieldDescription, TrivialMultipleDescription, Struct

GIVE_SAME_FIELD_FOR_FUNCTION_DESCRIPTION = False


class TrivialDescription(SingleFieldDescription):
    def __init__(
            self,
            field: Field,
            target_item_type: ItemType,
            input_item_type: ItemType = ItemType.Auto,
            skip_errors: bool = False,
            logger: Optional[LoggerInterface] = None,
            default: Value = None,
    ):
        super().__init__(
            field=field,
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger, default=default,
        )

    def get_input_fields(self) -> list:
        return [self.get_target_field()]

    def get_value_from_item(self, item: Item) -> Value:
        return get_field_value_from_item(
            field=self.get_target_field_name(),
            item=item, item_type=self.get_input_item_type(),
            skip_errors=self.must_skip_errors(), logger=self.get_logger(), default=self.get_default_value(),
        )

    def get_mapper(self, struct: Struct = None, item_type: ItemType = ItemType.Auto, default: Value = None) -> Callable:
        field = self.get_target_field_name()
        if item_type in (ItemType.Auto, None):
            item_type = self.get_input_item_type()
        if item_type not in (ItemType.Auto, None):
            return item_type.get_field_getter(field, struct=struct, default=default)
        else:
            return lambda i: item_type.get_value_from_item(item=i, field=field, struct=struct, default=default)

    def get_output_field_types(self, struct: StructInterface) -> list:
        field_name = self.get_target_field_name()
        return [struct.get_field_description(field_name).get_value_type()]

    def get_linked_fields(self) -> list:
        return [self.get_target_field()]

    def get_brief_repr(self) -> str:
        return repr(self.get_target_field_name())

    def get_detailed_repr(self) -> str:
        return '{}({})'.format(self.__class__.__name__, repr(self.get_target_field()))


class AliasDescription(SingleFieldDescription):
    def __init__(
            self,
            alias: Field,
            source: Field,
            target_item_type: ItemType,
            input_item_type: ItemType = ItemType.Auto,
            skip_errors: bool = False,
            logger: Optional[LoggerInterface] = None,
            default: Value = None,
    ):
        self._source_field = source
        super().__init__(
            alias,
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger, default=default,
        )

    def get_function(self) -> Callable:
        return lambda i: i

    def get_mapper(self, struct: Struct = None, item_type: ItemType = ItemType.Auto, default: Value = None) -> Callable:
        field = self.get_source_name()
        if item_type in (ItemType.Auto, None):
            item_type = self.get_input_item_type()
        if item_type not in (ItemType.Auto, None):
            return item_type.get_field_getter(field, struct=struct, default=default)
        else:
            return lambda i: item_type.get_value_from_item(item=i, field=field, struct=struct, default=default)

    def get_source_field(self) -> Field:
        return self._source_field

    def get_source_name(self) -> Name:
        return get_name(self.get_source_field())

    def get_input_fields(self) -> list:
        return [self.get_source_field()]

    def get_value_from_item(self, item: Field) -> Value:
        return get_field_value_from_item(
            field=self.get_source_name(),
            item=item, item_type=self.get_input_item_type(),
            skip_errors=self.must_skip_errors(), logger=self.get_logger(), default=self.get_default_value(),
        )

    def get_output_field_types(self, struct) -> list:
        return [struct.get_field_description(f).get_value_type() for f in self.get_input_field_names()]

    def get_selection_tuple(self, including_target: bool = False) -> tuple:
        if including_target:
            return self.get_target_field_name(), self.get_source_name()
        else:
            return self.get_source_name(),

    def get_sql_expression(self) -> str:
        return '{source} as {target}'.format(source=self.get_source_name(), target=self.get_target_field_name())

    def __str__(self):
        source = self.get_source_name()
        alias = self.get_target_field_name()
        return '{alias}={source}'.format(alias=alias, source=source)


class RegularDescription(SingleFieldDescription):
    def __init__(
            self,
            target: Field,
            function: Callable,
            inputs: Array,
            target_item_type: ItemType,
            input_item_type: ItemType = ItemType.Auto,
            skip_errors: bool = False,
            logger: Optional[LoggerInterface] = None,
            default: Value = None,
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
        function, inputs = process_description(list_description)
        return cls(target=target, function=function, inputs=inputs, **kwargs)

    def get_function(self) -> Callable:
        return self._function

    def get_return_type(self) -> Optional[type]:
        return self.get_annotations().get('return')

    def get_input_fields(self) -> Array:
        return self._inputs

    def get_output_field_types(self, struct) -> list:
        return [self.get_return_type()]

    def get_value_from_item(self, item: Item) -> Value:
        return safe_apply_function(
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
            target: Field,
            function: Callable,
            give_same_field_to_input: bool = GIVE_SAME_FIELD_FOR_FUNCTION_DESCRIPTION,
            target_item_type: ItemType = ItemType.Auto,
            input_item_type: ItemType = ItemType.Auto,
            skip_errors: bool = False,
            logger: Optional[LoggerInterface] = None,
            default: Value = None,
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

    def get_input_fields(self) -> list:
        if self.give_same_field_to_input:
            return [self.get_target_field()]
        else:
            return [ALL]

    def get_input_field_types(self) -> dict:
        field_types = self.get_annotations().copy()
        field_types.pop('return')
        return field_types

    def get_value_from_item(self, item) -> Value:
        return safe_apply_function(
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
            target_item_type: ItemType,
            input_item_type: ItemType = ItemType.Auto,
            skip_errors: bool = False,
            logger: Optional[LoggerInterface] = None,
    ):
        super().__init__(
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger,
        )

    def get_output_fields(
            self,
            item_or_struct: Union[Item, StructInterface],
            item_type: ItemType = ItemType.Auto,
    ) -> Array:
        if isinstance(item_or_struct, StructInterface) or hasattr(item_or_struct, 'get_columns'):
            return item_or_struct.get_columns()
        else:  # isinstance(item_or_struct, Item)
            return get_fields_names_from_item(item_or_struct, item_type=item_type)

    def get_values_from_row(self, item: Item) -> Item:
        if self.get_target_item_type() == ItemType.Row:
            return item


class DropDescription(TrivialMultipleDescription):
    def __init__(
            self,
            drop_fields: Array,
            target_item_type: ItemType,
            input_item_type: ItemType = ItemType.Auto,
            skip_errors: bool = False,
            logger: Optional[LoggerInterface] = None,
    ):
        super().__init__(
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger,
        )
        self._drop_fields = drop_fields

    def get_drop_fields(self) -> Array:
        return self._drop_fields

    def get_output_fields(self, item_or_struct: Union[Item, StructInterface]) -> list:
        if isinstance(item_or_struct, StructInterface) or hasattr(item_or_struct, 'get_columns'):
            initial_fields = item_or_struct.get_columns()
        else:
            initial_fields = get_fields_names_from_item(item_or_struct, item_type=self.get_input_item_type())
        return [f for f in initial_fields if f not in self.get_drop_fields()]
