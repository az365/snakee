from abc import ABC, abstractmethod
from typing import Callable, Union

try:  # Assume we're a submodule in a package.
    from interfaces import ItemType, StructInterface, Item, Name, Field, Value, LoggerInterface, Array, Auto, AUTO
    from base.functions.arguments import get_name
    from base.classes.typing import FieldID
    from base.constants.chars import ALL, NOT_SET
    from functions.primary import items as it
    from functions.secondary.basic_functions import same
    from utils import selection as sf
    from content.items.simple_items import SelectableItem
    from content.fields.field_interface import FieldInterface, FieldType
    from content.struct.struct_interface import StructInterface
    from content.selection.abstract_expression import AbstractDescription, MultipleFieldDescription
    from content.selection.concrete_expression import AliasDescription, RegularDescription
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import ItemType, StructInterface, Item, Name, Field, Value, LoggerInterface, Array, Auto, AUTO
    from ...base.functions.arguments import get_name
    from ...base.classes.typing import FieldID
    from ...base.constants.chars import ALL, NOT_SET
    from ...functions.primary import items as it
    from ...functions.secondary.basic_functions import same
    from ...utils import selection as sf
    from ..items.simple_items import SelectableItem
    from ..fields.field_interface import FieldInterface, FieldType
    from ..struct.struct_interface import StructInterface
    from .abstract_expression import AbstractDescription, MultipleFieldDescription
    from .concrete_expression import AliasDescription, RegularDescription

TYPE_ERROR_MSG = 'Expected Field, Struct or Item, got {}'


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
            raise TypeError(TYPE_ERROR_MSG.format(self))

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
        assert isinstance(self, FieldInterface) and isinstance(self, SelectableMixin), 'got {}'.format(self)
        return RegularDescription(
            target=self,
            function=same(),
            inputs=fields,
            target_item_type=self._get_target_item_type(),
            input_item_type=self._get_input_item_type(),
        )
