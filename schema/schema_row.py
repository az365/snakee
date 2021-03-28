from typing import Optional, Union, Iterable, Any

try:  # Assume we're a sub-module in a package.
    from base.abstract.simple_data import SimpleDataWrapper
    from connectors.databases import dialect as di
    from items.struct_row_interface import StructRowInterface
    from fields.schema_interface import SchemaInterface
    from schema.schema_description import SchemaDescription
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..base.abstract.simple_data import SimpleDataWrapper
    from ..connectors.databases import dialect as di
    from ..items.struct_row_interface import StructRowInterface
    from fields.schema_interface import SchemaInterface
    from .schema_description import SchemaDescription

SimpleRow = Union[list, tuple]
StructRow = Optional[StructRowInterface]
FieldName = str
FieldNo = int
FieldId = Union[FieldName, FieldNo]
Field = Union[FieldId, SimpleDataWrapper]


class SchemaRow(SimpleDataWrapper, StructRowInterface):
    def __init__(
            self,
            data: SimpleRow = [],
            schema: Union[SimpleRow, SchemaInterface] = [],
            check=True,
    ):
        if not isinstance(schema, SchemaInterface):
            schema = SchemaDescription(schema)
        self._schema = schema
        if check:
            data = self._schematize_row(data, schema)
        super().__init__(data=data, name='-')

    def get_schema(self) -> SchemaDescription:
        return self._schema

    def get_fields_descriptions(self) -> Iterable:
        return self.get_schema().get_fields_descriptions()

    def get_data(self) -> SimpleRow:
        return super().get_data()

    @staticmethod
    def _schematize_row(row: SimpleRow, schema: SchemaInterface) -> SimpleRow:
        assert isinstance(row, (list, tuple)), 'Row must be list or tuple (got {})'.format(type(row))
        expected_len = schema.get_fields_count()
        row_len = len(row)
        assert row_len == expected_len, 'count of cells must match the schema ({} != {})'.format(row_len, expected_len)
        schematized_fields = list()
        for value, desc in zip(row, schema.get_fields_descriptions()):
            if not desc.check_value(value):
                converter = desc.get_converter('str', 'py')
                value = converter(value)
            schematized_fields.append(value)
        return schematized_fields

    def set_data(self, row: SimpleRow, check: bool = True, inplace: bool = True) -> StructRow:
        if check:
            row = self._schematize_row(row, self.get_schema())
        return super().set_data(data=row, inplace=inplace)

    def set_value(self, field: Field, value: Any, inplace: bool = True) -> StructRow:
        if isinstance(field, FieldName):
            field = self.get_schema().get_field_position(field)
        self.get_data()[field] = value
        if not inplace:
            return self

    def get_record(self) -> dict:
        return {k.name: v for k, v in zip(self.get_fields_descriptions(), self.get_data())}

    def get_line(self, dialect='str', delimiter='\t', need_quotes=False) -> str:
        assert dialect in di.DIALECTS
        list_str = list()
        for k, v in zip(self.get_fields_descriptions(), self.get_data()):
            convert = k.get_converter('py', dialect)
            value = convert(v)
            if need_quotes:
                if not isinstance(value, (int, float, bool)):
                    value = '"{}"'.format(value)
            list_str.append(str(value))
        return delimiter.join(list_str)

    def get_columns(self) -> SimpleRow:
        return self.get_schema().get_columns()

    def get_field_position(self, field: Field) -> Optional[FieldNo]:
        if isinstance(field, FieldNo):
            return field
        else:  # isinstance(field, FieldName):
            return self.get_schema().get_field_position(field)

    def get_fields_positions(self, fields: SimpleRow) -> SimpleRow:
        return [self.get_field_position(f) for f in fields]

    def get_value(self, field: FieldId, skip_missing=False, logger=None, default=None):
        position = self.get_field_position(field)
        try:
            return self.get_data()[position]
        except IndexError or TypeError:
            msg = 'Field {} does no exists in current row'.format(field)
            if skip_missing:
                if logger:
                    logger.log(msg)
                return default
            else:
                raise IndexError(msg)

    def get_values(self, fields: SimpleRow) -> SimpleRow:
        positions = self.get_fields_positions(fields)
        return [self.get_data()[p] for p in positions]

    def simple_select_fields(self, fields: SimpleRow) -> StructRowInterface:
        return SchemaRow(
            data=self.get_values(fields),
            schema=self.get_schema().simple_select_fields(fields)
        )
