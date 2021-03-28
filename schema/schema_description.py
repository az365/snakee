from typing import Optional, Union, Iterable

try:  # Assume we're a sub-module in a package.
    from fields.field_interface import FieldInterface
    from fields.schema_interface import SchemaInterface
    from fields import field_type as ft
    from schema import schema_classes as sh
    from connectors.databases import dialect as di
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..fields.field_interface import FieldInterface
    from ..fields.schema_interface import SchemaInterface
    from ..fields import field_type as ft
    from . import schema_classes as sh
    from ..connectors.databases import dialect as di

FieldName = str
FieldNo = int
FieldID = Union[FieldNo, FieldName]
Array = Union[list, tuple]
ARRAY_SUBTYPES = list, tuple


class SchemaDescription(SchemaInterface):
    def __init__(self, fields_descriptions: Iterable):
        self.fields_descriptions = list()
        for field in fields_descriptions:
            self.append_field(field)

    def append_field(self, field, default_type=None, before=False):
        if isinstance(field, FieldInterface):
            field_desc = field
        elif isinstance(field, str):
            field_desc = sh.FieldDescription(field, default_type)
        elif isinstance(field, ARRAY_SUBTYPES):
            field_desc = sh.FieldDescription(*field)
        elif isinstance(field, dict):
            field_desc = sh.FieldDescription(**field)
        else:
            raise TypeError('Expected Field or str, got {} as {}'.format(field, type(field)))
        if before:
            self.fields_descriptions = [field_desc] + self.fields_descriptions
        else:
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
            '{} {}'.format(c.get_name(), c.get_type_in(dialect))
            for c in self.fields_descriptions
        ]
        return ', '.join(field_strings)

    def __repr__(self):
        return '[{}]'.format(self.get_schema_str())

    def __str__(self):
        return '<{}({})>'.format(self.__class__.__name__, self.get_schema_str())

    def get_columns(self):
        return [c.get_name() for c in self.fields_descriptions]

    def get_types(self, dialect):
        return [c.get_type_in(dialect) for c in self.fields_descriptions]

    def set_types(self, dict_field_types=None, return_schema=True, **kwargs):
        for field_name, field_type in list((dict_field_types or {}).items()) + list(kwargs.items()):
            field_description = self.get_field_description(field_name)
            assert isinstance(field_description, FieldInterface)
            field_description.set_type(ft.get_canonic_type(field_type), inplace=True)
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

    def get_field_description(self, field_name: FieldID) -> FieldInterface:
        field_position = self.get_field_position(field_name)
        return self.get_fields_descriptions()[field_position]

    def get_fields_descriptions(self):
        return self.fields_descriptions

    def is_valid_row(self, row):
        for value, field_type in zip(row, self.get_types('py')):
            if not isinstance(value, field_type):
                if not (field_type in (int, float) and not value):  # 0 is not float
                    return False
        return True

    def copy(self):
        return SchemaDescription(self.fields_descriptions)

    def simple_select_fields(self, fields: Array):
        return SchemaDescription(
            [self.get_field_description(f) for f in fields]
        )
