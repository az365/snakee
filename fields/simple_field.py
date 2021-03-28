from typing import Optional, Union

try:  # Assume we're a sub-module in a package.
    from fields.field_type import FieldType, FIELD_TYPES, DIALECTS, get_canonic_type
    from fields.schema_interface import SchemaInterface
    from fields.abstract_field import AbstractField
    from fields import field_classes as fc
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .field_type import FieldType, FIELD_TYPES, DIALECTS, get_canonic_type
    from .schema_interface import SchemaInterface
    from .abstract_field import AbstractField
    from fields import field_classes as fc


class SimpleField(AbstractField):
    def __init__(self, name: str, field_type: FieldType = FieldType.Any, properties=None):
        self._type = get_canonic_type(field_type)
        super().__init__(name=name, properties=properties)

    def __add__(self, other: Union[AbstractField, SchemaInterface, str]) -> SchemaInterface:
        if isinstance(other, str):
            return fc.FieldGroup([self, SimpleField(other)])
        elif isinstance(other, AbstractField):
            return fc.FieldGroup([self, other])
        elif isinstance(other, SchemaInterface):
            return other.append_field(self, before=True)
        else:
            raise TypeError('Expected other as field or schema, got {} as {}'.format(other, type(other)))
