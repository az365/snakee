from abc import ABC, abstractmethod
from typing import Callable, Sequence, Union

try:  # Assume we're a submodule in a package.
    from interfaces import ItemType, StructInterface, Item, FieldID, Field, Value
    from base.constants.chars import ALL, NOT_SET
    from base.functions.errors import get_type_err_msg
    from functions.secondary import all_secondary_functions as fs
    from content.fields.field_interface import FieldInterface
    from content.selection.abstract_expression import AbstractDescription
    from content.selection.concrete_expression import AliasDescription, RegularDescription
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import ItemType, StructInterface, Item, FieldID, Field, Value
    from ...base.constants.chars import ALL, NOT_SET
    from ...base.functions.errors import get_type_err_msg
    from ...functions.secondary import all_secondary_functions as fs
    from ..fields.field_interface import FieldInterface
    from .abstract_expression import AbstractDescription
    from .concrete_expression import AliasDescription, RegularDescription


class SelectableInterface(ABC):
    @abstractmethod
    def to(self, field: Field) -> AbstractDescription:
        pass

    @abstractmethod
    def map(self, func: Callable) -> AbstractDescription:
        pass


class SelectableMixin(SelectableInterface, ABC):
    def _get_target_item_type(self) -> ItemType:
        if hasattr(self, 'get_target_item_type'):
            return self.get_target_item_type()
        elif hasattr(self, '_target_item_type'):
            return self._target_item_type
        elif hasattr(self, 'get_item_type'):
            return self.get_item_type()
        else:
            return ItemType.Auto

    def _get_input_item_type(self) -> ItemType:
        if hasattr(self, 'get_input_item_type'):
            return self.get_input_item_type()
        elif hasattr(self, '_input_item_type'):
            return self._input_item_type
        elif hasattr(self, 'get_item_type'):
            return self.get_item_type()
        else:
            return ItemType.Auto

    def _get_input_fields(self, as_list: bool) -> Union[Field, list]:
        if isinstance(self, FieldInterface):
            return [self] if as_list else self
        elif isinstance(self, StructInterface):
            return self.get_fields_descriptions()
        elif isinstance(self, Item):
            if as_list:
                if hasattr(self, 'get_struct'):
                    return self.get_struct().get_fields_descriptions()
                else:
                    return [ALL]
            else:
                return ALL
        else:
            expected = FieldInterface, StructInterface, Item
            msg = get_type_err_msg(expected=expected, got=self, arg='self', caller=self._get_input_fields)
            raise TypeError(msg)

    def to(self, field: Field) -> AliasDescription:
        return AliasDescription(
            alias=field,
            source=self._get_input_fields(as_list=False),
            target_item_type=self._get_target_item_type(),
        )

    def map(self, function: Callable) -> RegularDescription:
        return RegularDescription(
            target=NOT_SET,
            function=function,
            inputs=self._get_input_fields(as_list=True),
            target_item_type=self._get_target_item_type(),
            input_item_type=self._get_input_item_type(),
        )

    def get_from(self, *fields) -> RegularDescription:
        if isinstance(self, FieldInterface) and isinstance(self, SelectableMixin):
            return RegularDescription(
                function=fs.same(),
                target=self, target_item_type=self._get_target_item_type(),
                inputs=fields, input_item_type=self._get_input_item_type(),
            )
        else:
            msg = get_type_err_msg(self, expected=(FieldInterface, SelectableMixin), arg='self', caller=self.get_from)
            raise TypeError(msg)

    def _get_comparison(self, func, arg) -> RegularDescription:
        function = func()
        if isinstance(arg, FieldInterface):
            inputs = [self, arg]
        elif isinstance(arg, StructInterface):
            inputs = [self, *arg]
        else:
            inputs = [self]
            function = func(arg)
        return RegularDescription(
            target=NOT_SET, target_item_type=self.get_default_item_type(),
            inputs=inputs, input_item_type=ItemType.Auto,
            function=function, default=None,
            skip_errors=self._skip_errors, logger=self._logger,
        )

    def equal(self, smth: Union[FieldInterface, StructInterface, Value]) -> RegularDescription:
        return self._get_comparison(fs.equal, smth)

    def not_equal(self, smth: Union[FieldInterface, StructInterface, Value]) -> RegularDescription:
        return self._get_comparison(fs.not_equal, smth)

    def is_in(self, array: Sequence) -> RegularDescription:
        return self._get_comparison(fs.is_in, array)

    def not_in(self, array: Sequence) -> RegularDescription:
        return self._get_comparison(fs.not_in, array)
