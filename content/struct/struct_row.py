from typing import Optional, Iterable, Callable, Union

try:  # Assume we're a submodule in a package.
    from base.functions.arguments import get_name
    from interfaces import (
        FieldInterface, StructInterface,
        ValueType, DialectType, ItemType,
        RECORD_SUBCLASSES, ROW_SUBCLASSES, Row, Line, Name, Field, FieldNo, Value, Auto, AUTO,
    )
    from base.abstract.simple_data import SimpleDataWrapper
    from content.struct.struct_row_interface import StructRowInterface, DEFAULT_DELIMITER
    from content.struct.flat_struct import FlatStruct
    from content.struct.struct_mixin import StructMixin
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.functions.arguments import get_name
    from ...interfaces import (
        FieldInterface, StructInterface,
        ValueType, DialectType, ItemType,
        RECORD_SUBCLASSES, ROW_SUBCLASSES, Row, Line, Name, Field, FieldNo, Value, Auto, AUTO,
    )
    from ...base.abstract.simple_data import SimpleDataWrapper
    from .struct_row_interface import StructRowInterface, DEFAULT_DELIMITER
    from .flat_struct import FlatStruct
    from .struct_mixin import StructMixin


class StructRow(SimpleDataWrapper, StructMixin, StructRowInterface):
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

    def set_data(self, row: Row, inplace: bool, check: bool = True, **kwargs) -> Optional[StructRowInterface]:
        if check:
            row = self._structure_row(row, self.get_struct())
        return super().set_data(data=row, inplace=inplace, **kwargs)

    def keys(self) -> Iterable:
        return map(get_name, self.get_fields_descriptions())

    def get_keys(self) -> list:
        return list(self.keys())

    def get_values(self, fields: Row) -> Row:
        positions = self.get_fields_positions(fields)
        return [self.get_data()[p] for p in positions]

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

    def set_value(
            self,
            field: Field,
            value: Value,
            field_type: Union[ValueType, Auto] = AUTO,
            update_struct: bool = False,
            inplace: bool = True,
    ) -> Optional[StructRowInterface]:
        if isinstance(field, FieldNo):
            field_position = field
        else:
            field_position = self.get_field_position(field)
        if field_position is None:
            if update_struct:
                self.get_struct().append_field(field, field_type, inplace=True)
                self.get_data().append(value)
                field_position = self.get_field_position(field)
            else:
                msg = 'field {} not found in {} and struct is locked (update_struct={})'
                raise ValueError(msg.format(field, self.get_struct(), update_struct))
        self.get_data()[field_position] = value
        if not inplace:
            return self

    def get(self, field: Field, default=None) -> Value:
        return self.get_value(field, skip_missing=True, default=default)

    def get_slice(self, start: FieldNo, stop: Optional[FieldNo] = None, step: Optional[FieldNo] = None) -> Row:
        return [self.get_value(i) for i in range(start, stop, step)]

    def simple_select_fields(self, fields: Row) -> StructRowInterface:
        data = self.get_values(fields)
        struct = self.get_struct().simple_select_fields(fields)
        return StructRow(data, struct=struct, check=False)

    def get_record(self) -> dict:
        return {k.name: v for k, v in zip(self.get_fields_descriptions(), self.get_data())}

    def get_line(
            self,
            dialect: DialectType = DialectType.String,
            delimiter: str = DEFAULT_DELIMITER,
            need_quotes: bool = False,
    ) -> str:
        list_str = list()
        for k, v in zip(self.get_fields_descriptions(), self.get_data()):
            convert = k.get_converter(DialectType.Python, dialect)
            value = convert(v)
            if need_quotes:
                if not isinstance(value, (int, float, bool)):
                    value = '"{}"'.format(value)
            list_str.append(str(value))
        return delimiter.join(list_str)

    def copy(self):
        data = self.get_data().copy()
        struct = self.get_struct().copy()
        return StructRow(data, struct=struct, check=False)

    def is_defined(self) -> bool:
        return bool(self.get_data()) and bool(self.get_struct())

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

    def __iter__(self):
        yield from self.get_data()

    def __len__(self):
        return self.get_struct().get_column_count()

    def __getitem__(self, item):
        if isinstance(item, slice):
            return self.get_slice(start=item.start, stop=item.stop, step=item.step)
        elif isinstance(item, ROW_SUBCLASSES):
            return self.get_values(item)
        else:
            return self.get_value(item)

    def __setitem__(self, key, value):
        self.set_value(key, value, update_struct=True, inplace=True)

    def __add__(self, other: StructRowInterface):
        assert isinstance(other, StructRowInterface), 'can add only StructRow, got {}'.format(other)
        return StructRow(
            data=list(self.get_data()) + list(other.get_data()),
            struct=self.get_struct() + other.get_struct(),
        )

    def __repr__(self):
        return str(self)

    def __str__(self):
        field_names = self.get_columns()
        field_values = self.get_values(field_names)
        field_strings = ['{}={}'.format(k, v.__repr__()) for k, v in zip(field_names, field_values)]
        return '{}({})'.format(self.__class__.__name__, ', '.join(field_strings))


ItemType.prepare()
ItemType.add_classes(StructRow)
ItemType.set_dict_classes(
    {
        ItemType.Line: [Line],
        ItemType.Row: ROW_SUBCLASSES,
        ItemType.Record: RECORD_SUBCLASSES,
        ItemType.StructRow: [StructRow, StructRowInterface],
    }
)
