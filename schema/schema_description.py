from typing import Union, Optional

try:  # Assume we're a sub-module in a package.
    from connectors.databases import dialect as di
    from fields import field_type as ft
    from schema.field_description import FieldDescription
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..connectors.databases import dialect as di
    from ..fields import field_type as ft
    from .field_description import FieldDescription

FieldName = str
FieldNo = int
FieldID = Union[FieldNo, FieldName]
Array = Union[list, tuple]
ARRAY_SUBTYPES = list, tuple


class SchemaDescription:
    def __init__(
            self,
            fields_descriptions,
    ):
        assert isinstance(fields_descriptions, ARRAY_SUBTYPES)
        self.fields_descriptions = list()
        for field in fields_descriptions:
            self.append_field(field)

    def append_field(self, field, default_type=None):
        if isinstance(field, FieldDescription):
            field_desc = field
        elif isinstance(field, str):
            field_desc = FieldDescription(field, default_type)
        elif isinstance(field, ARRAY_SUBTYPES):
            field_desc = FieldDescription(*field)
        elif isinstance(field, dict):
            field_desc = FieldDescription(**field)
        else:
            raise TypeError
        self.fields_descriptions.append(field_desc)

    def add_fields(self, *fields, default_type=None, return_schema=True):
        for f in fields:
            self.append_field(f, default_type=default_type)
        if return_schema:
            return self

    def get_fields_count(self):
        return len(self.fields_descriptions)

    def get_schema_str(self, dialect='py'):
        if dialect is not None and dialect not in di.DIALECTS:
            dialect = di.get_dialect_for_connector(dialect)
        field_strings = [
            '{} {}'.format(c.name, c.get_type_in(dialect))
            for c in self.fields_descriptions
        ]
        return ', '.join(field_strings)

    def get_columns(self):
        return [c.name for c in self.fields_descriptions]

    def get_types(self, dialect):
        return [c.get_type_in(dialect) for c in self.fields_descriptions]

    def set_types(self, dict_field_types=None, return_schema=True, **kwargs):
        for field_name, field_type in list((dict_field_types or {}).items()) + list(kwargs.items()):
            field_description = self.get_field_description(field_name)
            assert isinstance(field_description, FieldDescription)
            field_description.field_type = ft.get_canonic_type(field_type)
        if return_schema:
            return self

    def get_field_position(self, field: FieldID) -> Optional[FieldNo]:
        if isinstance(field, FieldNo):
            if field < self.get_fields_count():
                return field
        elif isinstance(field, FieldName):
            try:
                return self.get_columns().index(field)
            except ValueError or IndexError:
                return None

    def get_fields_positions(self, names: Array):
        columns = self.get_columns()
        return [columns.index(f) for f in names]

    def get_converters(self, src='str', dst='py'):
        converters = list()
        for desc in self.fields_descriptions:
            converters.append(desc.get_converter(src, dst))
        return tuple(converters)

    def get_field_description(self, field_name):
        field_position = self.get_field_position(field_name)
        return self.fields_descriptions[field_position]

    def get_fields_descriptions(self):
        return self.fields_descriptions

    def is_valid_row(self, row):
        for value, field_type in zip(row, self.get_types('py')):
            if not isinstance(value, field_type):
                return False
        return True

    def copy(self):
        return SchemaDescription(self.fields_descriptions)

    def simple_select_fields(self, fields: Array):
        return SchemaDescription(
            [self.get_field_description(f) for f in fields]
        )
