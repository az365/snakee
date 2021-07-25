from typing import Optional, Union, Iterable, Any

try:  # Assume we're a sub-module in a package.
    from interfaces import SchemaInterface, StructRowInterface, Row, Name, Field, FieldInterface
    from base.abstract.simple_data import SimpleDataWrapper
    from connectors.databases import dialect as di
    from items.flat_struct import FlatStruct
    from items.legacy_struct import LegacyStruct
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..interfaces import SchemaInterface, StructRowInterface, Row, Name, Field, FieldInterface
    from ..base.abstract.simple_data import SimpleDataWrapper
    from ..connectors.databases import dialect as di
    from .flat_struct import FlatStruct
    from .legacy_struct import LegacyStruct


class StructRow(SimpleDataWrapper, StructRowInterface):
    def __init__(
            self,
            data: Row,
            struct: Union[Row, SchemaInterface],
            check=True,
    ):
        if not isinstance(struct, SchemaInterface):
            struct = FlatStruct(struct)
        self._struct = struct
        if check:
            data = self._structure_row(data, struct)
        super().__init__(data=data, name='-')

    def get_struct(self) -> FlatStruct:
        return self._struct

    def get_schema(self) -> FlatStruct:
        return self.get_struct()

    def get_fields_descriptions(self) -> Iterable:
        return self.get_struct().get_fields_descriptions()

    def get_data(self) -> Row:
        return super().get_data()

    @staticmethod
    def _structure_row(row: Row, struct: SchemaInterface) -> Row:
        assert isinstance(row, (list, tuple)), 'Row must be list or tuple (got {})'.format(type(row))
        expected_len = astruct.get_fields_count()
        row_len = len(row)
        assert row_len == expected_len, 'count of cells must match the struct ({} != {})'.format(row_len, expected_len)
        structurized_fields = list()
        for value, desc in zip(row, struct.get_fields_descriptions()):
            if not desc.check_value(value):
                converter = desc.get_converter('str', 'py')
                value = converter(value)
            structurized_fields.append(value)
        return structurized_fields

    def set_data(self, row: Row, check: bool = True, inplace: bool = True) -> Optional[StructRowInterface]:
        if check:
            row = self._structure_row(row, self.get_struct())
        return super().set_data(data=row, inplace=inplace)

    def set_value(self, field: Field, value: Any, inplace: bool = True) -> Optional[StructRowInterface]:
        if isinstance(field, str):
            field = self.get_struct().get_field_position(field)
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

    def get_columns(self) -> Row:
        return self.get_struct().get_columns()

    def get_field_position(self, field: Field) -> Optional[int]:
        if isinstance(field, int):
            return field
        else:  # isinstance(field, str):
            return self.get_struct().get_field_position(field)

    def get_fields_positions(self, fields: Row) -> Row:
        return [self.get_field_position(f) for f in fields]

    def get_value(self, field: Name, skip_missing=False, logger=None, default=None):
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

    def get_values(self, fields: Row) -> Row:
        positions = self.get_fields_positions(fields)
        return [self.get_data()[p] for p in positions]

    def simple_select_fields(self, fields: Row) -> StructRowInterface:
        return StructRow(
            data=self.get_values(fields),
            struct=self.get_struct().simple_select_fields(fields)
        )


SchemaRow = StructRow
