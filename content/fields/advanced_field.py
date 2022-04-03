from typing import Optional, Callable, Iterable, Union, Any

try:  # Assume we're a submodule in a package.
    from content.fields.any_field import AnyField, RepresentationInterface, SelectionLogger, ValueType, ItemType, AUTO
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .any_field import AnyField, RepresentationInterface, SelectionLogger, ValueType, ItemType, AUTO


class AdvancedField(AnyField):
    def __init__(
            self,
            name: str,
            value_type=AUTO,
            field_type=AUTO,
            representation: Union[RepresentationInterface, str, None] = None,
            caption: str = '',
            default_value: Any = None,
            default: Any = None,
            extractors: Optional[Iterable] = None,
            transform: Optional[Callable] = None,
            skip_errors: bool = False,
            logger: Optional[SelectionLogger] = None,
            default_item_type=AUTO,
            target_item_type=AUTO,
            is_valid=AUTO,
            group_name: Optional[str] = None,
            group_caption: Optional[str] = None,
    ):
        assert not extractors, 'extractors-option no longer supported'
        assert not transform, 'transform-option no longer supported'
        super().__init__(
            name=name,
            caption=caption,
            value_type=AUTO.multi_acquire(value_type, field_type, ValueType.Any),
            representation=representation,
            default_value=default_value or default,
            default_item_type=AUTO.multi_acquire(default_item_type, target_item_type, ItemType.Any),
            skip_errors=skip_errors,
            is_valid=is_valid,
            logger=logger,
            group_name=group_name,
            group_caption=group_caption,
        )
