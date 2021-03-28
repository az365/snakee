from typing import Iterable

try:  # Assume we're a sub-module in a package.
    from items.struct_row_interface import StructRowInterface
    from fields.schema_interface import SchemaInterface
    from schema.field_description import FieldDescription
    from schema.schema_description import SchemaDescription
    from schema.schema_row import SchemaRow
    from fields.field_type import (
        FieldType,
        FIELD_TYPES, DIALECTS, AGGR_HINTS, HEURISTIC_SUFFIX_TO_TYPE,
        any_to_bool, safe_converter, get_canonic_type, detect_field_type_by_name,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..items.struct_row_interface import StructRowInterface
    from fields.schema_interface import SchemaInterface
    from .field_description import FieldDescription
    from .schema_description import SchemaDescription
    from .schema_row import SchemaRow
    from ..fields.field_type import (
        FieldType,
        FIELD_TYPES, DIALECTS, AGGR_HINTS, HEURISTIC_SUFFIX_TO_TYPE,
        any_to_bool, safe_converter, get_canonic_type, detect_field_type_by_name,
    )


def detect_schema_by_title_row(title_row: Iterable) -> SchemaInterface:
    schema = SchemaDescription([])
    for name in title_row:
        field_type = detect_field_type_by_name(name)
        schema.append_field(
            FieldDescription(name, field_type)
        )
    return schema
