try:  # Assume we're a submodule in a package.
    from content.fields.abstract_field import AbstractField, FieldType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .abstract_field import AbstractField, FieldType


class SimpleField(AbstractField):
    def __init__(self, name: str, field_type: FieldType = FieldType.Any, properties=None):
        super().__init__(name=name, field_type=field_type, properties=properties)
