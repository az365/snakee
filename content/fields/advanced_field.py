from typing import Optional, Union, Iterable, Callable, Any

try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from interfaces import (
        FieldInterface, RepresentationInterface, StructInterface, ExtLogger, SelectionLogger,
        FieldType, ReprType, ItemType,
        Field, Class,
        AutoBool, Auto, AUTO, ARRAY_TYPES,
    )
    from content.fields.abstract_field import AbstractField
    from content.selection import abstract_expression as ae, concrete_expression as ce
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from utils import arguments as arg
    from interfaces import (
        FieldInterface, RepresentationInterface, StructInterface, ExtLogger, SelectionLogger,
        FieldType, ReprType, ItemType,
        Field, Class,
        AutoBool, Auto, AUTO, ARRAY_TYPES,
    )
    from .abstract_field import AbstractField
    from ..selection import abstract_expression as ae
    from ..selection import concrete_expression as ce

Native = AbstractField

META_MEMBER_MAPPING = dict(_type='field_type', _data='extractors')


class AdvancedField(AbstractField):
    def __init__(
            self,
            name: str,
            field_type: FieldType = FieldType.Any,
            representation: Union[RepresentationInterface, str, None] = None,
            caption: Optional[str] = None,
            default: Any = None,
            extractors: Optional[Iterable] = None,
            transform: Optional[Callable] = None,
            skip_errors: bool = False,
            logger: Optional[SelectionLogger] = None,
            target_item_type: ItemType = ItemType.Any,
            is_valid: AutoBool = AUTO,
            group_name: Optional[str] = None,
            group_caption: Optional[str] = None,
    ):
        self._caption = caption
        self._representation = representation
        self._default = default
        self._transform = transform
        self._target_item_type = target_item_type
        self._skip_errors = skip_errors
        self._logger = logger
        self._is_valid = is_valid
        self._group_name = group_name or ''
        self._group_caption = group_caption or ''
        super().__init__(name=name, field_type=field_type, properties=extractors or list())

    @classmethod
    def _get_meta_member_mapping(cls) -> dict:
        return META_MEMBER_MAPPING

    def get_representation(self) -> Union[RepresentationInterface, str, None]:
        return self._representation

    def set_representation(self, representation: Union[RepresentationInterface, str], inplace: bool) -> Native:
        if inplace:
            self._representation = representation
            return self
        else:
            return self.make_new(representation=representation)

    def get_caption(self) -> str:
        return self._caption or None

    def set_caption(self, caption: str, inplace: bool) -> Native:
        if inplace:
            self._caption = caption
            return self
        else:
            return self.make_new(caption=caption)

    def caption(self, caption: str) -> Native:
        self._caption = caption
        return self

    def is_valid(self) -> AutoBool:
        return self._is_valid

    def set_valid(self, is_valid: bool, inplace: bool) -> Optional[Native]:
        if inplace:
            self._is_valid = is_valid
        else:
            return self.make_new(is_valid=is_valid)

    def valid(self, is_valid: bool) -> Native:
        self._is_valid = is_valid
        return self

    def check_value(self, value) -> bool:
        return self.get_type().check_value(value)

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

    def extract(self, extractor: ae.AbstractDescription) -> Native:
        self.get_extractors().append(extractor)
        return self

    def transform(self, func: Callable) -> Native:
        self._transform = func
        return self

    def to(self, target: Union[str, AbstractField]) -> ae.AbstractDescription:
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
