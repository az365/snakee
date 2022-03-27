from typing import Optional, Callable, Iterable, Union, Any

try:  # Assume we're a submodule in a package.
    from interfaces import (
        RepresentationInterface, SelectionLogger,
        FieldType, FieldRoleType, ItemType,
        AutoBool, Auto, AUTO,
    )
    from content.fields.advanced_field import AdvancedField, EMPTY
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        RepresentationInterface, SelectionLogger,
        FieldType, FieldRoleType, ItemType,
        AutoBool, Auto, AUTO,
    )
    from .advanced_field import AdvancedField, EMPTY


class RateField(AdvancedField):
    def __init__(
            self,
            name: str,
            field_type: FieldType = FieldType.Float,
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
        super().__init__(
            name=name, field_type=field_type, caption=caption,
            representation=representation, default=default,
            extractors=extractors, transform=transform,
            skip_errors=skip_errors, logger=logger,
            target_item_type=target_item_type, is_valid=is_valid,
            group_name=group_name, group_caption=group_caption,
        )

    @staticmethod
    def get_role() -> FieldRoleType:
        return FieldRoleType.Rate
