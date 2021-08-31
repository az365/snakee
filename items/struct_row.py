from typing import Optional, Iterable, Callable, Union, Any

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from interfaces import ROW_SUBCLASSES, Row, Name, Field, FieldNo, FieldInterface, StructInterface
    from base.abstract.simple_data import SimpleDataWrapper
    from connectors.databases import dialect as di
    from items.struct_row_interface import StructRowInterface, DEFAULT_DELIMITER
    from items.flat_struct import FlatStruct
    from items.legacy_struct import LegacyStruct
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..interfaces import ROW_SUBCLASSES, Row, Name, Field, FieldNo, FieldInterface, StructInterface
    from ..base.abstract.simple_data import SimpleDataWrapper
    from ..connectors.databases import dialect as di
    from .struct_row_interface import StructRowInterface, DEFAULT_DELIMITER
    from .flat_struct import FlatStruct
    from .legacy_struct import LegacyStruct


class StructRow(SimpleDataWrapper, StructRowInterface):
    def __init__(
            self,
            data: Row,
            struct: Union[Row, StructInterface],
            check: bool = True,
    ):
        if not isinstance(struct, StructInterface):
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
    def _structure_row(row: Row, struct: StructInterface) -> Row:
        assert isinstance(row, ROW_SUBCLASSES), 'Row must be list or tuple (got {})'.format(type(row))
        expected_len = struct.get_fields_count()
        row_len = len(row)
        assert row_len == expected_len, 'count of cells must match the struct ({} != {})'.format(row_len, expected_len)
        structured_fields = list()
        for value, desc in zip(row, struct.get_fields_descriptions()):
            if not desc.check_value(value):
                converter = desc.get_converter('str', 'py')
                value = converter(value)
            structured_fields.append(value)
        return structured_fields

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

    def get_line(self, dialect='str', delimiter=DEFAULT_DELIMITER, need_quotes=False) -> str:
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

    def get_field_position(self, field: Field) -> Optional[FieldNo]:
        if isinstance(field, FieldNo):
            return field
        else:
            field_name = arg.get_name(field)
            return self.get_struct().get_field_position(field_name)

    def get_fields_positions(self, fields: Row) -> Row:
        return [self.get_field_position(f) for f in fields]

    def get_value(self, field: Union[Name, Callable], skip_missing: bool = False, logger=None, default=None):
        if isinstance(field, Callable):
            return field(self)
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

    def get_slice(self, start: FieldNo, stop: Optional[FieldNo] = None, step: Optional[FieldNo] = None) -> Row:
        return [self.get_value(i) for i in range(start, stop, step)]

    def simple_select_fields(self, fields: Row) -> StructRowInterface:
        return StructRow(
            data=self.get_values(fields),
            struct=self.get_struct().simple_select_fields(fields)
        )

    def __iter__(self):
        return self.get_data()

    def __len__(self):
        return self.get_struct().get_column_count()

    def __getitem__(self, item):
        if isinstance(item, slice):
            return self.get_slice(start=item.start, stop=item.stop, step=item.step)
        elif isinstance(item, ROW_SUBCLASSES):
            return self.get_values(item)
        else:
            return self.get_value(item)

    def __add__(self, other: StructRowInterface):
        assert isinstance(other, StructRowInterface), 'can add only StructRow, got {}'.format(other)
        return StructRow(
            data=list(self.get_data()) + list(other.get_data()),
            struct=self.get_struct() + other.get_struct(),
        )


SchemaRow = StructRow