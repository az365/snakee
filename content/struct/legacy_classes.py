from typing import Iterable, Union

try:  # Assume we're a submodule in a package.
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
