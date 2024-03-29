from typing import Optional, Union, Any

try:  # Assume we're a submodule in a package.
    from interfaces import RepresentationInterface, SelectionLogger, ValueType, FieldRoleType, ItemType
    from content.fields.any_field import AnyField, EMPTY
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import RepresentationInterface, SelectionLogger, ValueType, FieldRoleType, ItemType
    from .any_field import AnyField, EMPTY


class NormField(AnyField):
    def __init__(
            self,
            name: str,
            value_type: ValueType = ValueType.Float,
            representation: Union[RepresentationInterface, str, None] = None,
            caption: Optional[str] = None,
            default_value: Any = None,
            skip_errors: bool = False,
            logger: Optional[SelectionLogger] = None,
            default_item_type: ItemType = ItemType.Any,
            is_valid: Optional[bool] = None,
            group_name: Optional[str] = None,
            group_caption: Optional[str] = None,
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

    @staticmethod
    def get_role() -> FieldRoleType:
        return FieldRoleType.Norm


FieldRoleType.add_classes(norm=NormField)
