from typing import Union

try:  # Assume we're a submodule in a package.
    from interfaces import StructInterface, FieldType
    from content.fields.abstract_field import AbstractField
    from content.fields import field_classes as fc
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import StructInterface, FieldType
    from .abstract_field import AbstractField
    from . import field_classes as fc


class SimpleField(AbstractField):
    def __init__(self, name: str, field_type: FieldType = FieldType.Any, properties=None):
        super().__init__(name=name, field_type=field_type, properties=properties)

    def __add__(self, other: Union[AbstractField, StructInterface, str]) -> StructInterface:
        if isinstance(other, str):
            return fc.FlatStruct([self, SimpleField(other)])
        elif isinstance(other, AbstractField):
            return fc.FlatStruct([self, other])
        elif isinstance(other, StructInterface):
            struct = other.append_field(self, before=True, inplace=False)
            assert isinstance(struct, StructInterface), struct
            return struct
        else:
            raise TypeError('Expected other as field or struct, got {} as {}'.format(other, type(other)))
