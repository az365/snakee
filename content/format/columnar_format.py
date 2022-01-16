from typing import Optional, Union, Iterable, Generator, Callable

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from utils.decorators import deprecated_with_alternative
    from interfaces import (
        Item, Record, Row, StructRow, StructInterface,
        ItemType, StreamType, ContentType,
        AUTO, Auto, AutoBool, Array, ARRAY_TYPES,
    )
    from functions.secondary import item_functions as fs
    from content.format.text_format import TextFormat, Compress, DEFAULT_ENDING, DEFAULT_ENCODING
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...utils.decorators import deprecated_with_alternative
    from ...interfaces import (
        Item, Record, Row, StructRow, StructInterface,
        ItemType, StreamType, ContentType,
        AUTO, Auto, AutoBool, Array, ARRAY_TYPES,
    )
    from ...functions.secondary import item_functions as fs
    from .text_format import TextFormat, Compress, DEFAULT_ENDING, DEFAULT_ENCODING

DEFAULT_IS_FIRST_LINE_TITLE = True
DEFAULT_DELIMITER = '\t'
POPULAR_DELIMITERS = '\t', '; ', ', ', ';', ',', ' '


class ColumnarFormat(TextFormat):
    def __init__(
            self,
            first_line_is_title: bool = DEFAULT_IS_FIRST_LINE_TITLE,
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

    def set_first_line_title(self, first_line_is_title: AutoBool) -> TextFormat:
        self._first_line_is_title = arg.acquire(first_line_is_title, DEFAULT_IS_FIRST_LINE_TITLE)
        return self

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
        if item_type == ItemType.Line:
            return line
        line_parser = fs.csv_loads(delimiter=self.get_delimiter())
        row = line_parser(line)
        if isinstance(struct, StructInterface):
            field_converters = struct.get_converters()
            row_converter = self._get_row_converter(converters=field_converters)
            row = row_converter(row)
        if item_type in (ItemType.Row, ItemType.Any, ItemType.Auto):
            return row
        if not arg.is_defined(struct, check_name=False):
            column_count = len(row)
            struct = list(range(column_count))
        if item_type == ItemType.Record:
            return {arg.get_name(k): v for k, v in zip(struct, row)}
        elif item_type == ItemType.StructRow:
            return ItemType.StructRow.build(data=row, struct=struct)
        else:
            msg = 'item_type {} is not supported for {}.parse_lines()'
            raise ValueError(msg.format(item_type, self.__class__.__name__))

    def get_items_from_lines(
            self,
            lines: Iterable,
            item_type: Union[ItemType, Auto] = AUTO,
            struct: Union[Array, StructInterface, Auto] = AUTO,
    ) -> Generator:
        item_type = arg.delayed_acquire(item_type, self.get_default_item_type)
        if item_type in (ItemType.Record, ItemType.Row, ItemType.StructRow, ItemType.Any, ItemType.Auto):
            iter_parser = fs.csv_reader(delimiter=self.get_delimiter())
            rows = iter_parser(lines)
            if isinstance(struct, StructInterface):
                column_names = struct.get_columns()
                field_converters = struct.get_converters()
                rows = map(self._get_row_converter(converters=field_converters), rows)
            elif isinstance(struct, ARRAY_TYPES):
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
                assert arg.is_defined(struct, check_name=False)
                for r in rows:
                    yield ItemType.StructRow.build(data=r, struct=struct)
        else:  # item_type == ItemType.Line
            for line in lines:
                yield self.get_parsed_line(line, item_type=item_type, struct=struct)


class FlatStructFormat(ColumnarFormat):
    def __init__(
            self,
            struct: Union[StructInterface, Auto] = AUTO,
            first_line_is_title: bool = DEFAULT_IS_FIRST_LINE_TITLE,
            delimiter: str = DEFAULT_DELIMITER,
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

    def get_content_type(self) -> ContentType:
        if self.get_delimiter() == DEFAULT_DELIMITER:
            return ContentType.TsvFile
        else:
            return ContentType.CsvFile

    def get_struct(self) -> StructInterface:
        return self._struct

    def set_struct(self, struct: StructInterface, inplace: bool) -> Optional[ColumnarFormat]:
        if inplace:
            self._struct = struct
        else:
            return self.make_new(struct=struct)

    def get_default_stream_type(self) -> StreamType:
        return StreamType.RecordStream

    def get_default_item_type(self) -> ItemType:
        return ItemType.Record

    def _get_validated_struct(
            self,
            struct: Union[Array, StructInterface, Auto] = AUTO,
    ) -> Union[Array, StructInterface]:
        if arg.is_defined(struct, check_name=False):
            if isinstance(struct, ARRAY_TYPES):
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
            item_columns = item.get_struct().get_columns()
            content_columns = self.get_struct().get_columns()
            assert item_columns == content_columns, '{} != {}'.format(item_columns, content_columns)
        return super().get_formatted_item(item, item_type=item_type)

    def get_parsed_line(
            self,
            line: str,
            item_type: Union[ItemType, Auto] = AUTO,
            struct: Union[Array, StructInterface, Auto] = AUTO,
    ) -> Item:
        struct = self._get_validated_struct(struct)
        return super().get_parsed_line(line, item_type=item_type, struct=struct)

    def get_items_from_lines(
            self,
            lines: Iterable,
            item_type: Union[ItemType, Auto] = AUTO,
            struct: Union[Array, StructInterface, Auto] = AUTO,
    ) -> Generator:
        struct = self._get_validated_struct(struct)
        return super().get_items_from_lines(lines, item_type=item_type, struct=struct)

    def copy(self):
        struct = self.get_struct()
        if isinstance(struct, StructInterface) or hasattr(struct, 'copy'):
            struct = struct.copy()
        return self.make_new(struct=struct)