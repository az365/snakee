try:  # Assume we're a submodule in a package.
    from utils.decorators import deprecated_with_alternative
    from content.value_type import ValueType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.decorators import deprecated_with_alternative
    from ..value_type import ValueType


# deprecated
FieldType = ValueType


@deprecated_with_alternative('FieldType.get_canonic_type()')
def get_canonic_type(field_type, ignore_missing: bool = False) -> FieldType:
    field_type = FieldType.get_canonic_type(field_type, ignore_missing=ignore_missing)
    assert isinstance(field_type, FieldType)
    return field_type


@deprecated_with_alternative('FieldType.detect_by_name()')
def detect_field_type_by_name(field_name) -> FieldType:
    field_type = FieldType.detect_by_name(field_name)
    assert isinstance(field_type, FieldType)
    return field_type
