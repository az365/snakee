from typing import Optional, Union, Iterable, Callable, Any

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from items.base_item_type import ItemType
    from fields.field_type import FieldType
    from fields.schema_interface import SchemaInterface
    from fields.abstract_field import AbstractField
    from fields import field_classes as fc
    from selection.abstract_expression import AbstractDescription
    from selection import concrete_expression as ce
    from loggers.selection_logger_interface import SelectionLoggerInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..items.base_item_type import ItemType
    from .field_type import FieldType
    from .schema_interface import SchemaInterface
    from .abstract_field import AbstractField
    from . import field_classes as fc
    from ..selection.abstract_expression import AbstractDescription
    from ..selection import concrete_expression as ce
    from ..loggers.selection_logger_interface import SelectionLoggerInterface


class AdvancedField(AbstractField):
    def __init__(
            self, name: str, field_type: FieldType = FieldType.Any,
            caption: Optional[str] = None, default: Any = None,
            extractors: Optional[Iterable] = None, transform: Optional[Callable] = None,
            skip_errors: bool = False, logger: Optional[SelectionLoggerInterface] = None,
            target_item_type: ItemType = ItemType.Any,
    ):
        self._caption = caption
        self._default = default
        self._transform = transform
        self._target_item_type = target_item_type
        self._skip_errors = skip_errors
        self._logger = logger
        super().__init__(name=name, properties=extractors or list(), field_type=field_type)

    def get_caption(self) -> str:
        return self._caption or None

    def set_caption(self, caption: str, inplace: bool):
        if inplace:
            self._caption = caption
        else:
            return self.make_new(caption=caption)

    def caption(self, caption: str):
        self._caption = caption
        return self

    def get_extractors(self):
        return self.get_data()

    def extract(self, extractor: AbstractDescription):
        self.get_extractors().append(extractor)

    def transform(self, func: Callable):
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
            inputs=[self],
            input_item_type=ItemType.Auto,
            function=field_type.convert_value, default=self._default,
            skip_errors=self._skip_errors, logger=self._logger,
        )

    def drop(self):
        return ce.DropDescription([self], target_item_type=ItemType.Auto)

    def __add__(self, other: Union[AbstractField, SchemaInterface, str]) -> SchemaInterface:
        if isinstance(other, str):
            return fc.FieldGroup([self, AdvancedField(other)])
        elif isinstance(other, AbstractField):
            return fc.FieldGroup([self, other])
        elif isinstance(other, SchemaInterface):
            return other.append_field(self, before=True)
        else:
            raise TypeError('Expected other as field or schema, got {} as {}'.format(other, type(other)))
