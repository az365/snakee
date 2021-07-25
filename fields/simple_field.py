from typing import Union

try:  # Assume we're a sub-module in a package.
    from fields.field_type import FieldType, FIELD_TYPES, DIALECTS, get_canonic_type
    from items.struct_interface import StructInterface
    from fields.abstract_field import AbstractField
    from fields import field_classes as fc
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .field_type import FieldType, FIELD_TYPES, DIALECTS, get_canonic_type
    from ..items.struct_interface import StructInterface
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
            return other.append_field(self, before=True)
        else:
            raise TypeError('Expected other as field or struct, got {} as {}'.format(other, type(other)))
