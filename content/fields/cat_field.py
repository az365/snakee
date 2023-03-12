from typing import Optional, Sequence, Union, Any

try:  # Assume we're a submodule in a package.
    from interfaces import RepresentationInterface, SelectionLogger, ValueType, FieldRoleType, ItemType
    from base.classes.enum import DynamicEnum
    from content.fields.any_field import AnyField, FieldEdgeType, EMPTY
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import RepresentationInterface, SelectionLogger, ValueType, FieldRoleType, ItemType
    from ...base.classes.enum import DynamicEnum
    from .any_field import AnyField, FieldEdgeType, EMPTY

Native = AnyField
OptEnum = Union[DynamicEnum, Sequence, None]


class CatField(AnyField):
    def __init__(
            self,
            name: str,
            value_type: ValueType = ValueType.Str,
            representation: Union[RepresentationInterface, str, None] = None,
            caption: Optional[str] = None,
            default_value: Any = None,
            skip_errors: bool = False,
            logger: Optional[SelectionLogger] = None,
            default_item_type: ItemType = ItemType.Any,
            is_valid: Optional[bool] = None,
            group_name: Optional[str] = None,
            group_caption: Optional[str] = None,
            enum: OptEnum = None,
            data: Optional[dict] = None,
    ):
        super().__init__(
            name=name, value_type=value_type, caption=caption,
            representation=representation, default_value=default_value,
            skip_errors=skip_errors, logger=logger,
            default_item_type=default_item_type, is_valid=is_valid,
            group_name=group_name, group_caption=group_caption,
            data=data,
        )
        self.set_enum(enum)

    @staticmethod
    def get_role() -> FieldRoleType:
        return FieldRoleType.Cat

    def get_enum(self) -> OptEnum:
        enum_value = self.get_from_data(key=FieldEdgeType.Enum)
        return enum_value

    def set_enum(self, enum: OptEnum) -> Native:
        field = self.add_to_data(key=FieldEdgeType.Enum, value=enum)
        return self._assume_native(field)

    @staticmethod
    def _assume_native(field) -> Native:
        return field


FieldRoleType.add_classes(cat=CatField)
