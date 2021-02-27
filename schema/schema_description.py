try:  # Assume we're a sub-module in a package.
    from schema import schema_classes as sh
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from . import schema_classes as sh


class SchemaDescription:
    def __init__(
            self,
            fields_descriptions,
    ):
        assert isinstance(fields_descriptions, (list, tuple))
        self.fields_descriptions = list()
        for field in fields_descriptions:
            self.append_field(field)

    def append_field(self, field, default_type=None):
        if isinstance(field, sh.FieldDescription):
            field_desc = field
        elif isinstance(field, str):
            field_desc = sh.FieldDescription(field, default_type)
        elif isinstance(field, (list, tuple)):
            field_desc = sh.FieldDescription(*field)
        elif isinstance(field, dict):
            field_desc = sh.FieldDescription(**field)
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
        if dialect is not None and dialect not in sh.DIALECTS:
            dialect = sh.get_dialect_for_conn_type(dialect)
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
            assert isinstance(field_description, sh.FieldDescription)
            field_description.field_type = sh.get_canonic_type(field_type)
        if return_schema:
            return self

    def get_field_position(self, name):
        try:
            return self.get_columns().index(name)
        except ValueError:
            return None

    def get_fields_positions(self, names):
        columns = self.get_columns()
        return [columns.index(f) for f in names]

    def get_converters(self, from_='str', to_='py'):
        converters = list()
        for desc in self.fields_descriptions:
            converters.append(desc.get_converter(from_, to_))
        return tuple(converters)

    def get_field_description(self, field_name):
        field_position = self.get_field_position(field_name)
        return self.fields_descriptions[field_position]

    def is_valid_row(self, row):
        for value, field_type in zip(row, self.get_types('py')):
            if not isinstance(value, field_type):
                return False
        return True

    def copy(self):
        return SchemaDescription(self.fields_descriptions)
