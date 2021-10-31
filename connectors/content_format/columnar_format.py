import csv
from typing import Optional, Union, Iterable, Generator, Iterator, Callable

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from interfaces import (
        Item, Record, Row, StructRow, StructInterface,
        ItemType, StreamType, ContentType,
        AUTO, Auto, AutoBool, Array,
    )
    from connectors.content_format.text_format import TextFormat, Compress, DEFAULT_ENDING, DEFAULT_ENCODING
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...interfaces import (
        Item, Record, Row, StructRow, StructInterface,
        ItemType, StreamType, ContentType,
        AUTO, Auto, AutoBool, Array,
    )
    from .text_format import TextFormat, Compress, DEFAULT_ENDING, DEFAULT_ENCODING

DEFAULT_DELIMITER = '\t'
POPULAR_DELIMITERS = '\t', '; ', ', ', ';', ',', ' '


class ColumnarFormat(TextFormat):
    def __init__(
            self,
            first_line_is_title: bool = True,
            delimiter: str = DEFAULT_DELIMITER,
            ending: str = DEFAULT_ENDING,
            encoding: str = DEFAULT_ENCODING,
            compress: Compress = None,
    ):
        assert isinstance(delimiter, str)
        self._delimiter = delimiter
        self._first_line_is_title = first_line_is_title
        super().__init__(ending=ending, encoding=encoding, compress=compress)

    @staticmethod
    def is_columnar() -> bool:
        return True

    @staticmethod
    def can_be_parsed() -> bool:
        return True

    def is_first_line_title(self) -> bool:
        return self._first_line_is_title

    def get_delimiter(self) -> str:
        return self._delimiter

    def set_delimiter(self, delimiter: str, inplace: bool) -> Optional[TextFormat]:
        if inplace:
            self._delimiter = delimiter
        else:
            return self.make_new(delimiter=delimiter)

    @staticmethod
    def detect_delimiter_by_example_line(line: str, expected_delimiters=POPULAR_DELIMITERS) -> str:
        for delimiter in expected_delimiters:
            if delimiter in line:
                return delimiter
        return DEFAULT_DELIMITER

    def get_default_stream_type(self) -> StreamType:
        return StreamType.RowStream

    def get_default_item_type(self) -> ItemType:
        return ItemType.Row

    def set_struct(self, struct: StructInterface, inplace: bool) -> TextFormat:
        if inplace:
            raise ValueError('for ColumnarFormat struct can not be set inplace, use inplace=False instead')
        else:
            return FlatStructFormat(struct=struct, **self.get_props())

    def get_formatted_item(self, item: Item, item_type: Union[ItemType, Auto] = AUTO) -> str:
        if not arg.is_defined(item_type):
            item_type = ItemType.detect(item)
        if item_type in (ItemType.Row, ItemType.StructRow):
            row = item
            if hasattr(row, 'get_data'):
                row = row.get_data()
        elif item_type in (ItemType.Line, ItemType.Any):
            row = [item]
        elif item_type == ItemType.Record:
            raise ValueError('For format record to row struct mus be defined in FlatStructFormat')
        else:
            raise ValueError('item_type {} not supported for ColumnarFormat'.format(item_type))
        row = map(str, row)
        return self.get_delimiter().join(row)

    def _get_rows_from_csv(self, lines: Iterable) -> Iterator:
        if self.get_delimiter():
            return csv.reader(lines, delimiter=self.get_delimiter())
        else:
            return csv.reader(lines)

    def _parse_csv_line(self, line: str) -> Row:
        for row in self._get_rows_from_csv(lines=[line]):
            return row

    @staticmethod
    def _get_row_converter(converters: Row) -> Callable:
        return lambda r: [c(v) for c, v in zip(converters, r)]

    def get_parsed_line(
            self,
            line: str,
            item_type: Union[ItemType, Auto] = AUTO,
            struct: Union[Array, StructInterface, Auto] = AUTO,
    ) -> Item:
        item_type = arg.delayed_acquire(item_type, self.get_default_item_type)
        if item_type in (ItemType.Record, ItemType.Row, ItemType.StructRow, ItemType.Any, ItemType.Auto):
            row = self._parse_csv_line(line)
            if isinstance(struct, StructInterface):
                field_converters = struct.get_converters()
                row_converter = self._get_row_converter(converters=field_converters)
                row = row_converter(row)
            if item_type in (ItemType.Row, ItemType.Any, ItemType.Auto):
                return row
            if not arg.is_defined(struct):
                column_count = len(row)
                struct = list(range(column_count))
            if item_type == ItemType.Record:
                return {k: v for k, v in zip(struct, row)}
            elif item_type == ItemType.StructRow:
                return ItemType.StructRow.build(data=row, struct=struct)
        elif item_type == ItemType.Line:
            return line
        msg = 'item_type {} is not supported for {}.parse_lines()'
        raise ValueError(msg.format(item_type, self.__class__.__name__))

    def get_items(
            self,
            lines: Iterable,
            item_type: Union[ItemType, Auto] = AUTO,
            struct: Union[Array, StructInterface, Auto] = AUTO,
    ) -> Generator:
        item_type = arg.delayed_acquire(item_type, self.get_default_item_type)
        if item_type in (ItemType.Record, ItemType.Row, ItemType.StructRow, ItemType.Any, ItemType.Auto):
            rows = self._get_rows_from_csv(lines)
            if isinstance(struct, StructInterface):
                column_names = struct.get_columns()
                field_converters = struct.get_converters()
                rows = map(self._get_row_converter(converters=field_converters), rows)
            elif isinstance(struct, Array):
                column_names = struct
            else:
                column_names = None
            if item_type in (ItemType.Row, ItemType.Any, ItemType.Auto):
                yield from rows
            elif item_type == ItemType.Record:
                for r in rows:
                    if column_names:
                        yield {k: v for k, v in zip(column_names, r)}
                    else:
                        yield {k: v for k, v in enumerate(r)}
            elif item_type == ItemType.StructRow:
                assert arg.is_defined(struct)
                for r in rows:
                    yield ItemType.StructRow.build(data=r, struct=struct)
        else:  # item_type == ItemType.Line
            for line in lines:
                yield self.get_parsed_line(line, item_type=item_type, struct=struct)


class FlatStructFormat(ColumnarFormat):
    def __init__(
            self,
            struct: StructInterface,
            first_line_is_title: bool,
            delimiter: str,
            ending: str = DEFAULT_ENDING,
            encoding: str = DEFAULT_ENCODING,
            compress: Compress = None,
    ):
        self._struct = struct
        super().__init__(
            first_line_is_title=first_line_is_title,
            delimiter=delimiter, ending=ending,
            encoding=encoding, compress=compress,
        )

    def get_struct(self) -> StructInterface:
        return self._struct

    def set_struct(self, struct: StructInterface, inplace: bool) -> Optional[ColumnarFormat]:
        if inplace:
            self._struct = struct
        else:
            return self.make_new(struct=struct)

    def get_default_stream_type(self) -> StreamType:
        return StreamType.StructStream

    def get_default_item_type(self) -> ItemType:
        return ItemType.StructRow

    def _get_validated_struct(
            self,
            struct: Union[Array, StructInterface, Auto] = AUTO,
    ) -> Union[Array, StructInterface]:
        if arg.is_defined(struct):
            if isinstance(struct, Array):
                assert struct == self.get_struct().get_columns()
        else:
            struct = self.get_struct()
        return struct

    def get_lines(self, items: Iterable, item_type: ItemType, add_title_row: AutoBool = AUTO) -> Generator:
        add_title_row = arg.undefault(add_title_row, self.is_first_line_title())
        if add_title_row:
            assert self.is_first_line_title()
            title_row = self.get_struct().get_columns()
            yield self.get_formatted_item(title_row, item_type=ItemType.Row)
        for i in items:
            yield self.get_formatted_item(i, item_type=item_type)

    def get_formatted_item(self, item: Item, item_type: Union[ItemType, Auto] = AUTO, validate=True) -> str:
        if not arg.is_defined(item_type):
            item_type = ItemType.detect(item)
        if item_type == ItemType.Record:
            row = [str(item.get(f)) for f in self.get_struct().get_columns()]
            return self.get_delimiter().join(row)
        if item_type == ItemType.StructRow and validate:
            assert item.get_struct() == self.get_struct()
        return super().get_formatted_item(item, item_type=item_type)

    def get_parsed_line(
            self,
            line: str,
            item_type: Union[ItemType, Auto] = AUTO,
            struct: Union[Array, StructInterface, Auto] = AUTO,
    ) -> Item:
        struct = self._get_validated_struct(struct)
        return super().get_parsed_line(line, item_type=item_type, struct=struct)

    def get_items(
            self,
            lines: Iterable,
            item_type: Union[ItemType, Auto] = AUTO,
            struct: Union[Array, Auto] = AUTO,
    ) -> Generator:
        struct = self._get_validated_struct(struct)
        return super().get_items(lines, item_type=item_type, struct=struct)

    def copy(self):
        return self.make_new(struct=self.get_struct().copy())
