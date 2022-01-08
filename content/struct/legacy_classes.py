from typing import Iterable, Union

try:  # Assume we're a sub-module in a package.
    from utils.arguments import any_to_bool, safe_converter
    from interfaces import StructInterface, StructRowInterface, StructRow, FieldType
    from content.fields.legacy_field import LegacyField
    from content.struct.legacy_struct import LegacyStruct
    from content.struct.struct_row import StructRow
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.arguments import any_to_bool, safe_converter
    from ...interfaces import StructInterface, StructRowInterface, StructRow, FieldType
    from ..fields.legacy_field import LegacyField
    from .legacy_struct import LegacyStruct
    from .struct_row import StructRow

FieldDescription = LegacyField
SchemaDescription = LegacyStruct
SchemaInterface = StructInterface

NAME_POS, TYPE_POS, HINT_POS = 0, 1, 2  # old style struct fields
DICT_CAST_TYPES = dict(bool=bool, int=int, float=float, str=str, text=str, date=str)


def detect_schema_by_title_row(title_row: Iterable) -> StructInterface:
    struct = LegacyStruct([])
    for name in title_row:
        field_type = FieldType.detect_by_name(name)
        assert isinstance(field_type, FieldType)
        struct.append_field(
            LegacyField(name, field_type)
        )
    return struct


def get_validation_errors(row: Iterable, struct: Union[StructInterface, Iterable], default_type=str):
    if isinstance(struct, (LegacyStruct, StructInterface)) or hasattr(struct, 'get_fields_descriptions'):
        iter_struct = struct.get_fields_descriptions()
    else:
        assert isinstance(struct, Iterable)
        iter_struct = struct
    validation_errors = list()
    names = list()
    types = list()
    for description in iter_struct:
        field_name = description[NAME_POS]
        field_type = description[TYPE_POS]
        names.append(field_name)
        if field_type not in DICT_CAST_TYPES.values():
            field_type = DICT_CAST_TYPES.get(field_type, default_type)
        types.append(field_type)
    for value, field_name, field_type in zip(row, names, types):
        if not isinstance(value, field_type):
            template = 'Field {}: type {} expected, got {} (value={})'
            msg = template.format(field_name, field_type, type(value), value)
            validation_errors.append(msg)
    return validation_errors
