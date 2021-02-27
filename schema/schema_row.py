try:  # Assume we're a sub-module in a package.
    from schema import schema_classes as sh
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from . import schema_classes as sh


class SchemaRow:
    def __init__(
            self,
            data,
            schema,
            check=True,
    ):
        if isinstance(schema, sh.SchemaDescription):
            self.schema = schema
        else:
            self.schema = sh.SchemaDescription(schema)
        if check:
            self.data = list()
            self.set_data(data, check)
        else:
            self.data = data

    def set_data(self, row, check=True):
        if check:
            assert isinstance(row, (list, tuple)), 'Row must be list or tuple (got {})'.format(type(row))
            expected_fields_count = self.schema.get_fields_count()
            assert len(row) == expected_fields_count, 'count of cells must match the schema ({} != {})'.format(
                len(row), expected_fields_count,
            )
            schematized_fields = list()
            for value, desc in zip(row, self.schema.fields_descriptions):
                if not desc.check_value(value):
                    converter = desc.get_converter('str', 'py')
                    value = converter(value)
                schematized_fields.append(value)
            self.data = schematized_fields
        else:
            self.data = row

    def get_record(self):
        return {k.name: v for k, v in zip(self.schema.fields_descriptions, self.data)}

    def get_line(self, dialect='str', delimiter='\t', need_quotes=False):
        assert dialect in sh.DIALECTS
        list_str = list()
        for k, v in zip(self.schema.fields_descriptions, self.data):
            convert = k.get_converter('py', dialect)
            value = convert(v)
            if need_quotes:
                if not isinstance(value, (int, float, bool)):
                    value = '"{}"'.format(value)
            list_str.append(str(value))
        return delimiter.join(list_str)

    def get_value(self, name, skip_errors=False, logger=None, default=None):
        position = self.schema.get_field_position(name)
        if position is None and isinstance(name, int):
            position = name
        try:
            return self.data[position]
        except IndexError or TypeError:
            msg = 'Field {} does no exists in current row'.format(name)
            if skip_errors:
                if logger:
                    logger.log(msg)
                return default
            else:
                raise IndexError(msg)

    def get_values(self, names):
        positions = self.schema.get_fields_positions(names)
        return [self.data[p] for p in positions]
