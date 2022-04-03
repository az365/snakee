try:  # Assume we're a submodule in a package.
    from utils.decorators import deprecated_with_alternative
    from content.fields.any_field import AnyField, ValueType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.decorators import deprecated_with_alternative
    from .any_field import AnyField, ValueType


class SimpleField(AnyField):
    @deprecated_with_alternative('AnyField')
    def __init__(self, name: str, value_type: ValueType = ValueType.Any):
        super().__init__(name=name, value_type=value_type)
