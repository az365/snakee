from abc import ABC

try:  # Assume we're a submodule in a package.
    from utils.decorators import deprecated_with_alternative
    from content.fields.any_field import AnyField, FieldType, FieldInterface, EMPTY
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.decorators import deprecated_with_alternative
    from .any_field import AnyField, FieldType, FieldInterface, EMPTY


class AbstractField(AnyField, ABC):
    @deprecated_with_alternative('AnyField')
    def __init__(self, name: str, value_type: FieldType = FieldType.Any, caption: str = EMPTY, properties=None):
        super().__init__(name=name, value_type=value_type, caption=caption, properties=properties)
