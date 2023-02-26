from typing import Optional, Union, Iterable, Generator, Callable

try:  # Assume we're a submodule in a package.
    from interfaces import Item, Row, StructInterface, ItemType, StreamType, ContentType, ARRAY_TYPES
    from base.constants.chars import PARAGRAPH_CHAR, TAB_CHAR
    from base.functions.arguments import get_name
    from functions.secondary import item_functions as fs
    from utils.decorators import deprecated_with_alternative
    from content.format.text_format import TextFormat, Compress, DEFAULT_ENCODING
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import Item, Row, StructInterface, ItemType, StreamType, ContentType, ARRAY_TYPES
    from ...base.constants.chars import PARAGRAPH_CHAR, TAB_CHAR
    from ...base.functions.arguments import get_name
    from ...functions.secondary import item_functions as fs
    from ...utils.decorators import deprecated_with_alternative
    from .text_format import TextFormat, Compress, DEFAULT_ENCODING

DEFAULT_IS_FIRST_LINE_TITLE = True
POPULAR_DELIMITERS = '\t', '; ', ', ', ';', ',', ' '


class ColumnarFormat(TextFormat):
    def __init__(
            self,
            first_line_is_title: bool = DEFAULT_IS_FIRST_LINE_TITLE,  # True
            delimiter: str = TAB_CHAR,  # '\t'
            ending: str = PARAGRAPH_CHAR,  # '\n'
            encoding: str = DEFAULT_ENCODING,  # 'utf8'
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

    def set_first_line_title(self, first_line_is_title: Optional[bool]) -> TextFormat:
        if first_line_is_title is None:
            first_line_is_title = DEFAULT_IS_FIRST_LINE_TITLE
        self._first_line_is_title = first_line_is_title
        return self

    def get_delimiter(self) -> str:
        return self._delimiter

    def set_delimiter(self, delimiter: str, inplace: bool) -> Optional[TextFormat]:
        if inplace:
            self._delimiter = delimiter
        else:
            return self.make_new(delimiter=delimiter)

    @staticmethod
    def detect_delimiter_by_example_line(
            line: str,
            expected_delimiters: Iterable = POPULAR_DELIMITERS,
            default: str = TAB_CHAR,
    ) -> str:
        for delimiter in expected_delimiters:
            if delimiter in line:
                return delimiter
        return default

    def get_default_stream_type(self) -> StreamType:
        return StreamType.RowStream

    def get_default_item_type(self) -> ItemType:
        return ItemType.Row

    def set_struct(self, struct: StructInterface, inplace: bool) -> TextFormat:
        if inplace:
            raise ValueError('for ColumnarFormat struct can not be set inplace, use inplace=False instead')
        else:
            return FlatStructFormat(struct=struct, **self.get_props())

    def get_formatted_item(self, item: Item, item_type: ItemType = ItemType.Auto) -> str:
        if item_type in (ItemType.Auto, None):
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
            raise ValueError(f'item_type {item_type} not supported for ColumnarFormat')
        row = map(str, row)
        return self.get_delimiter().join(row)

    @staticmethod
    def _get_row_converter(converters: Row) -> Callable:
        return lambda r: [c(v) for c, v in zip(converters, r)]

    def get_parsed_line(
            self,
            line: str,
            item_type: ItemType = ItemType.Auto,
            struct: Optional[StructInterface] = None,
    ) -> Item:
        if item_type in (ItemType.Auto, None):
            item_type = self.get_default_item_type()
        if item_type == ItemType.Line:
            return line
        line_parser = fs.csv_loads(delimiter=self.get_delimiter())
        row = line_parser(line)
        if isinstance(struct, StructInterface):
            field_converters = struct.get_converters()
            row_converter = self._get_row_converter(converters=field_converters)
            row = row_converter(row)
        if item_type in (ItemType.Row, ItemType.Any, ItemType.Auto, None):
            return row
        if struct is None:
            column_count = len(row)
            struct = list(range(column_count))
        if item_type == ItemType.Record:
            return {get_name(k): v for k, v in zip(struct, row)}
        elif item_type == ItemType.StructRow:
            return ItemType.StructRow.build(data=row, struct=struct)
        else:
            class_name = self.__class__.__name__
            raise ValueError(f'item_type {item_type} is not supported for {class_name}.parse_lines()')

    def get_items_from_lines(
            self,
            lines: Iterable,
            item_type: ItemType = ItemType.Auto,
            struct: Union[StructInterface] = None,
    ) -> Generator:
        if item_type in (ItemType.Auto, None):
            item_type = self.get_default_item_type()
        if item_type in (ItemType.Record, ItemType.Row, ItemType.StructRow, ItemType.Any, ItemType.Auto, None):
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
            if item_type in (ItemType.Row, ItemType.Any, ItemType.Auto, None):
                yield from rows
            elif item_type == ItemType.Record:
                for r in rows:
                    if column_names:
                        yield {k: v for k, v in zip(column_names, r)}
                    else:
                        yield {k: v for k, v in enumerate(r)}
            elif item_type == ItemType.StructRow:
                assert struct, f'for {item_type} struct must be defined, got struct={struct}'
                for r in rows:
                    yield ItemType.StructRow.build(data=r, struct=struct)
        else:  # item_type == ItemType.Line
            for line in lines:
                yield self.get_parsed_line(line, item_type=item_type, struct=struct)


class FlatStructFormat(ColumnarFormat):
    def __init__(
            self,
            struct: Optional[StructInterface] = None,
            first_line_is_title: bool = DEFAULT_IS_FIRST_LINE_TITLE,  # True
            delimiter: str = TAB_CHAR,  # '\t'
            ending: str = PARAGRAPH_CHAR,  # '\n'
            encoding: str = DEFAULT_ENCODING,  # 'utf8'
            compress: Compress = None,
    ):
        self._struct = struct
        super().__init__(
            first_line_is_title=first_line_is_title,
            delimiter=delimiter, ending=ending,
            encoding=encoding, compress=compress,
        )

    def get_content_type(self) -> ContentType:
        if self.get_delimiter() == TAB_CHAR:  # '\t'
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
            struct: Optional[StructInterface] = None,
    ) -> Union[list, StructInterface]:
        if struct is None:
            struct = self.get_struct()
        elif isinstance(struct, ARRAY_TYPES):
            assert struct == self.get_struct().get_columns()
        return struct

    def get_lines(self, items: Iterable, item_type: ItemType, add_title_row: Optional[bool] = None) -> Generator:
        if add_title_row is None:
            add_title_row = self.is_first_line_title()
        if add_title_row:
            assert self.is_first_line_title()
            title_row = self.get_struct().get_columns()
            yield self.get_formatted_item(title_row, item_type=ItemType.Row)
        for i in items:
            yield self.get_formatted_item(i, item_type=item_type)

    def get_formatted_item(self, item: Item, item_type: ItemType = ItemType.Auto, validate: bool = True) -> str:
        if item_type in (ItemType.Auto, None):
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
            item_type: ItemType = ItemType.Auto,
            struct: Optional[StructInterface] = None,
    ) -> Item:
        struct = self._get_validated_struct(struct)
        return super().get_parsed_line(line, item_type=item_type, struct=struct)

    def get_items_from_lines(
            self,
            lines: Iterable,
            item_type: ItemType = ItemType.Auto,
            struct: Optional[StructInterface] = None,
    ) -> Generator:
        struct = self._get_validated_struct(struct)
        return super().get_items_from_lines(lines, item_type=item_type, struct=struct)

    def copy(self):
        struct = self.get_struct()
        if isinstance(struct, StructInterface) or hasattr(struct, 'copy'):
            struct = struct.copy()
        return self.make_new(struct=struct)
