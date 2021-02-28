from typing import Union
from abc import ABC, abstractmethod

try:  # Assume we're a sub-module in a package.
    from utils import (
        arguments as arg,
        items as it,
        selection as sf,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import (
        arguments as arg,
        items as it,
        selection as sf,
    )


AUTO = it.ItemType.Auto
GIVE_SAME_FIELD_FOR_FUNCTION_DESCRIPTION = False

FieldID = Union[int, str]
Array = Union[list, tuple]


class AbstractDescription(ABC):
    def __init__(
            self,
            target_item_type: it.ItemType, input_item_type=AUTO,
            skip_errors=False, logger=None,
    ):
        self.target_type = target_item_type
        self.input_type = input_item_type
        self.skip_errors = skip_errors
        self.logger = logger

    def get_target_item_type(self):
        return self.target_type

    def get_input_item_type(self):
        return self.input_type

    def get_logger(self):
        return self.logger

    @abstractmethod
    def get_function(self):
        pass

    @abstractmethod
    def must_finish(self):
        pass

    @staticmethod
    def get_input_field_names(self):
        pass

    @abstractmethod
    def get_output_field_names(self, *args):
        pass

    @abstractmethod
    def get_output_field_types(self, schema):
        pass

    def get_dict_output_field_types(self, schema):
        return dict(zip(self.get_output_field_names(), self.get_output_field_types()))

    @abstractmethod
    def apply_inplace(self, item):
        pass


class SingleFieldDescription(AbstractDescription, ABC):
    def __init__(
            self,
            field: FieldID,
            target_item_type: it.ItemType, input_item_type=AUTO,
            skip_errors=False, logger=None, default=None,
    ):
        super().__init__(
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger,
        )
        assert isinstance(field, (int, str)), 'got {} as {}'.format(field, type(field))
        self.target = field
        self.default = default

    def must_finish(self):
        return False

    def get_target_field_name(self):
        return self.target

    def get_input_field_names(self):
        return []

    def get_input_values(self, item):
        return it.get_fields_values_from_item(
            fields=self.get_input_field_names(),
            item=item, item_type=self.get_input_item_type(),
            skip_errors=self.skip_errors, logger=self.logger, default=self.default,
        )

    def get_output_field_names(self, *args):
        yield self.get_target_field_name()

    def get_function(self):
        return lambda i: i

    @abstractmethod
    def get_value_from_item(self, item):
        pass

    def apply_inplace(self, item):
        assert self.get_target_item_type() == self.get_input_item_type()
        it.set_to_item_inplace(
            field=self.get_target_field_name(),
            value=self.get_value_from_item(item),
            item=item,
            item_type=arg.undefault(self.get_target_item_type(), it.ItemType.detect(item)),
        )


class MultipleFieldDescription(AbstractDescription, ABC):
    def __init__(
            self,
            target_item_type: it.ItemType, input_item_type=AUTO,
            skip_errors=False, logger=None,
    ):
        super().__init__(
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger,
        )


class TrivialMultipleDescription(MultipleFieldDescription, ABC):
    def __init__(
            self,
            target_item_type: it.ItemType, input_item_type=AUTO,
            skip_errors=False, logger=None,
    ):
        super().__init__(
            target_item_type=target_item_type, input_item_type=input_item_type,
            skip_errors=skip_errors, logger=logger,
        )

    def must_finish(self):
        return False

    @staticmethod
    def is_trivial_multiple():
        return True

    def get_function(self):
        return lambda i: i

    def get_input_field_names(self, item):
        return self.get_output_field_names(item)

    def get_output_field_types(self, schema):
        return [schema.get_field_description(f).get_field_type() for f in self.get_output_field_names()]

    def apply_inplace(self, item):
        pass
