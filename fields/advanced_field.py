from typing import Optional, Union, Iterable, Callable, Any

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from items.base_item_type import ItemType
    from fields.field_type import FieldType
    from items.struct_interface import StructInterface
    from fields.abstract_field import AbstractField
    from fields import field_classes as fc
    from selection.abstract_expression import AbstractDescription
    from selection import concrete_expression as ce
    from loggers.selection_logger_interface import SelectionLoggerInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..items.base_item_type import ItemType
    from .field_type import FieldType
    from ..items.struct_interface import StructInterface
    from .abstract_field import AbstractField
    from . import field_classes as fc
    from ..selection.abstract_expression import AbstractDescription
    from ..selection import concrete_expression as ce
    from ..loggers.selection_logger_interface import SelectionLoggerInterface

Native = AbstractField


class AdvancedField(AbstractField):
    def __init__(
            self, name: str, field_type: FieldType = FieldType.Any,
            caption: Optional[str] = None, default: Any = None,
            extractors: Optional[Iterable] = None, transform: Optional[Callable] = None,
            skip_errors: bool = False, logger: Optional[SelectionLoggerInterface] = None,
            target_item_type: ItemType = ItemType.Any,
            group_name: Optional[str] = None, group_caption: Optional[str] = None,
    ):
        self._caption = caption
        self._default = default
        self._transform = transform
        self._target_item_type = target_item_type
        self._skip_errors = skip_errors
        self._logger = logger
        self._group_name = group_name or ''
        self._group_caption = group_caption or ''
        super().__init__(name=name, field_type=field_type, properties=extractors or list())

    def get_caption(self) -> str:
        return self._caption or None

    def set_caption(self, caption: str, inplace: bool) -> Optional[Native]:
        if inplace:
            self._caption = caption
        else:
            return self.make_new(caption=caption)

    def caption(self, caption: str) -> Native:
        self._caption = caption
        return self

    def get_group_name(self) -> str:
        return self._group_name

    def set_group_name(self, group_name: str, inplace: bool) -> Optional[Native]:
        if inplace:
            self._group_name = group_name
        else:
            return self.make_new(group_name=group_name)

    def group_name(self, group_name: str) -> Native:
        self._group_name = group_name
        return self

    def get_group_caption(self) -> str:
        return self._group_caption

    def set_group_caption(self, group_caption: str, inplace: bool) -> Optional[Native]:
        if inplace:
            self._group_caption = group_caption
        else:
            return self.make_new(group_caption=group_caption)

    def group_caption(self, group_caption: str) -> Native:
        self._group_caption = group_caption
        return self

    def get_extractors(self) -> Optional[Iterable]:
        return self.get_data()

    def extract(self, extractor: AbstractDescription) -> Native:
        self.get_extractors().append(extractor)
        return self

    def transform(self, func: Callable) -> Native:
        self._transform = func
        return self

    def to(self, target: Union[str, AbstractField]) -> AbstractDescription:
        if self._transform:
            return ce.RegularDescription(
                target=target, target_item_type=self._target_item_type,
                inputs=[self], input_item_type=ItemType.Auto,
                function=self._transform, default=self._default,
                skip_errors=self._skip_errors, logger=self._logger,
            )
        else:
            return ce.AliasDescription(
                alias=target, target_item_type=self._target_item_type,
                source=self, input_item_type=ItemType.Auto,
                default=self._default,
                skip_errors=self._skip_errors, logger=self._logger,
            )

    def as_type(self, field_type: FieldType) -> ce.RegularDescription:
        return ce.RegularDescription(
            target=self.get_name(), target_item_type=self._target_item_type,
            inputs=[self], input_item_type=ItemType.Auto,
            function=field_type.convert, default=self._default,
            skip_errors=self._skip_errors, logger=self._logger,
        )

    def drop(self):
        return ce.DropDescription([self], target_item_type=ItemType.Auto)

    def __add__(self, other: Union[AbstractField, StructInterface, str]) -> StructInterface:
        if isinstance(other, str):
            return fc.FlatStruct([self, AdvancedField(other)])
        elif isinstance(other, AbstractField):
            return fc.FlatStruct([self, other])
        elif isinstance(other, StructInterface):
            return other.append_field(self, before=True, inplace=False)
        else:
            raise TypeError('Expected other as field or struct, got {} as {}'.format(other, type(other)))
