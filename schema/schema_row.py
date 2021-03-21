from typing import Union, Optional

try:  # Assume we're a sub-module in a package.
    from connectors.databases import dialect as di
    from schema.schema_description import SchemaDescription
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..connectors.databases import dialect as di
    from .schema_description import SchemaDescription


Row = Union[list, tuple]
FieldName = str
FieldNo = int
FieldId = Union[FieldName, FieldNo]


class SchemaRow:
    def __init__(
            self,
            data: Row = [],
            schema: Union[Row, SchemaDescription] = [],
            check=True,
    ):
        if isinstance(schema, SchemaDescription):
            self.schema = schema
        else:
            self.schema = SchemaDescription(schema)
        if check:
            self.data = list()
            self.set_data(data, check)
        else:
            self.data = data

    def get_schema(self) -> SchemaDescription:
        return self.schema

    def get_data(self) -> Row:
        return self.data

    def set_data(self, row: Row, check=True):
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

    def set_value(self, field: FieldId, value):
        if isinstance(field, FieldName):
            field = self.get_schema().get_field_position(field)
        self.data[field] = value

    def get_record(self):
        return {k.name: v for k, v in zip(self.schema.fields_descriptions, self.data)}

    def get_line(self, dialect='str', delimiter='\t', need_quotes=False):
        assert dialect in di.DIALECTS
        list_str = list()
        for k, v in zip(self.schema.fields_descriptions, self.data):
            convert = k.get_converter('py', dialect)
            value = convert(v)
            if need_quotes:
                if not isinstance(value, (int, float, bool)):
                    value = '"{}"'.format(value)
            list_str.append(str(value))
        return delimiter.join(list_str)

    def get_columns(self) -> Row:
        return self.get_schema().get_columns()

    def get_field_position(self, field: FieldId) -> Optional[FieldNo]:
        if isinstance(field, FieldNo):
            return field
        else:  # isinstance(field, FieldName):
            return self.get_schema().get_field_position(field)

    def get_fields_positions(self, fields: Row) -> Row:
        return [self.get_field_position(f) for f in fields]

    def get_value(self, field: FieldId, skip_missing=False, logger=None, default=None):
        position = self.get_field_position(field)
        try:
            return self.data[position]
        except IndexError or TypeError:
            msg = 'Field {} does no exists in current row'.format(field)
            if skip_missing:
                if logger:
                    logger.log(msg)
                return default
            else:
                raise IndexError(msg)

    def get_values(self, fields: Row) -> Row:
        positions = self.get_fields_positions(fields)
        return [self.data[p] for p in positions]

    def simple_select_fields(self, fields: Row):
        return SchemaRow(
            data=self.get_values(fields),
            schema=self.get_schema().simple_select_fields(fields)
        )
