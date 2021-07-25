from typing import Iterable

try:  # Assume we're a sub-module in a package.
    from interfaces import SchemaInterface, StructRowInterface, StructRow
    from fields.legacy_field import LegacyField
    from items.legacy_struct import LegacyStruct
    from schema.schema_row import SchemaRow
    from fields.field_type import (
        FieldType,
        FIELD_TYPES, DIALECTS, AGGR_HINTS, HEURISTIC_SUFFIX_TO_TYPE,
        any_to_bool, safe_converter, get_canonic_type, detect_field_type_by_name,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..interfaces import SchemaInterface, StructRowInterface, StructRow
    from ..fields.legacy_field import LegacyField
    from ..items.legacy_struct import LegacyStruct
    from ..schema.schema_row import SchemaRow
    from ..fields.field_type import (
        FieldType,
        FIELD_TYPES, DIALECTS, AGGR_HINTS, HEURISTIC_SUFFIX_TO_TYPE,
        any_to_bool, safe_converter, get_canonic_type, detect_field_type_by_name,
    )


FieldDescription = LegacyField
SchemaDescription = LegacyStruct


def detect_schema_by_title_row(title_row: Iterable) -> SchemaInterface:
    schema = LegacyStruct([])
    for name in title_row:
        field_type = detect_field_type_by_name(name)
        schema.append_field(
            LegacyField(name, field_type)
        )
    return schema
