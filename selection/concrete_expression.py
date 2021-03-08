from typing import Callable

try:  # Assume we're a sub-module in a package.
    from utils import (
        arguments as arg,
        items as it,
        selection as sf,
    )
    from selection import abstract_expression as ae
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import (
        arguments as arg,
        items as it,
        selection as sf,
    )
    from . import abstract_expression as ae


class TrivialDescription(ae.SingleFieldDescription):
    def __init__(
            self,
            field: ae.FieldID,
            target_item_type: it.ItemType, input_item_type=ae.AUTO,
            skip_errors=False, logger=None, default=None,
    ):
        super().__init__(
            field,
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger, default=default,
        )

    def get_input_field_names(self):
        return [self.get_target_field_name()]

    def get_value_from_item(self, item):
        return it.get_field_value_from_item(
            field=self.get_target_field_name(),
            item=item, item_type=self.get_input_item_type(),
            skip_errors=self.skip_errors, logger=self.logger, default=self.default,
        )

    def get_output_field_types(self, schema):
        field_name = self.get_target_field_name()
        return [schema.get_field_description(field_name).get_field_type()]


class AliasDescription(ae.SingleFieldDescription):
    def __init__(
            self,
            alias: ae.FieldID, source: ae.FieldID,
            target_item_type: it.ItemType, input_item_type=ae.AUTO,
            skip_errors=False, logger=None, default=None,
    ):
        super().__init__(
            alias,
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger, default=default,
        )
        self.input_name = source

    def get_function(self):
        return lambda i: i

    def get_source_name(self):
        return self.input_name

    def get_input_field_names(self):
        yield self.get_source_name()

    def get_value_from_item(self, item):
        return it.get_field_value_from_item(
            field=self.get_source_name(),
            item=item, item_type=self.get_input_item_type(),
            skip_errors=self.skip_errors, logger=self.logger, default=self.default,
        )

    def get_output_field_types(self, schema):
        return [schema.get_field_description(f).get_field_type() for f in self.get_input_field_names()]


class RegularDescription(ae.SingleFieldDescription):
    def __init__(
            self,
            target: ae.FieldID, function: Callable, inputs: ae.Array,
            target_item_type: it.ItemType, input_item_type=ae.AUTO,
            skip_errors=False, logger=None, default=None,
    ):
        super().__init__(
            target,
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger, default=default,
        )
        self.function = function
        self.inputs = inputs

    @classmethod
    def from_list(cls, target, list_description, **kwargs):
        function, inputs = sf.process_description(list_description)
        return cls(
            target=target, function=function, inputs=inputs,
            **kwargs,
        )

    def get_function(self):
        return self.function

    def get_return_type(self):
        try:
            return self.get_function().__annotations__.get('return')
        except AttributeError:
            pass

    def get_output_field_types(self, schema):
        return [self.get_return_type()]

    def get_input_field_names(self):
        return self.inputs

    def get_value_from_item(self, item):
        return sf.safe_apply_function(
            function=self.get_function(),
            fields=self.get_input_field_names(),
            values=self.get_input_values(item),
            item=item,
            logger=self.logger,
            skip_errors=self.skip_errors,
        )


class FunctionDescription(ae.SingleFieldDescription):
    def __init__(
            self,
            target: ae.FieldID, function: Callable,
            give_same_field_to_input=ae.GIVE_SAME_FIELD_FOR_FUNCTION_DESCRIPTION,
            target_item_type=it.ItemType, input_item_type=ae.AUTO,
            skip_errors=False, logger=None, default=None,
    ):
        super().__init__(
            target,
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger, default=default,
        )
        self.function = function
        self.give_same_field_to_input = give_same_field_to_input

    def get_function(self):
        return self.function

    def get_return_type(self):
        try:
            return self.get_function().__annotations__.get('return')
        except AttributeError:
            pass

    def get_output_field_types(self, schema):
        return [self.get_return_type()]

    def get_input_field_names(self):
        if self.give_same_field_to_input:
            return [self.get_target_field_name()]
        else:
            return [it.STAR]

    def get_input_field_types(self):
        field_types = self.get_function().__annotations__
        field_types.pop('return')
        return field_types

    def get_value_from_item(self, item):
        return sf.safe_apply_function(
            function=self.get_function(),
            fields=self.get_input_field_names(),
            values=self.get_input_values(item),
            item=item,
            logger=self.logger,
            skip_errors=self.skip_errors,
        )


class StarDescription(ae.TrivialMultipleDescription):
    def __init__(
            self,
            target_item_type: it.ItemType, input_item_type=ae.AUTO,
            skip_errors=False, logger=None,
    ):
        super().__init__(
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger,
        )

    def get_output_field_names(self, item, item_type=arg.DEFAULT):
        return it.get_fields_names_from_item(item, item_type=item_type)

    def get_values_from_row(self, item):
        if self.get_target_item_type() == it.ItemType.Row:
            return item


class DropDescription(ae.TrivialMultipleDescription):
    def __init__(
            self,
            drop_fields: ae.Array,
            target_item_type: it.ItemType, input_item_type=ae.AUTO,
            skip_errors=False, logger=None,
    ):
        super().__init__(
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger,
        )
        self.drop_fields = drop_fields

    def get_drop_fields(self):
        return self.drop_fields

    def get_output_field_names(self, item):
        return [
            f for f in it.get_fields_names_from_item(item, item_type=self.get_input_item_type())
            if f not in self.get_drop_fields()
        ]
