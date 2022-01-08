from typing import Optional, Union, Iterable, Iterator, NoReturn
import csv

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from interfaces import (
        StructInterface, Connector, LeafConnector, RegularStream, LineStream, RowStream, RecordStream, StructStream,
        FieldType, ItemType, StreamType, FileType, ConnType, DialectType,
        Field, Name, Item, Array, Columns,
        AUTO, Auto, AutoName, AutoCount, AutoBool, OptionalFields
    )
    from connectors.filesystem.local_file import LocalFile, AbstractFormat, ContentType
    from connectors.filesystem.text_file import TextFile, JsonFile
    from streams.mixin.columnar_mixin import ColumnarMixin
    from streams import stream_classes as sm
    from content.struct import legacy_classes as sh
    from utils.decorators import deprecated_with_alternative
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...interfaces import (
        StructInterface, Connector, LeafConnector, RegularStream, LineStream, RowStream, RecordStream, StructStream,
        FieldType, ItemType, StreamType, FileType, ConnType, DialectType,
        Field, Name, Item, Array, Columns,
        AUTO, Auto, AutoName, AutoCount, AutoBool, OptionalFields
    )
    from .local_file import LocalFile, AbstractFormat, ContentType
    from .text_file import TextFile, JsonFile
    from ...streams.mixin.columnar_mixin import ColumnarMixin
    from ...streams import stream_classes as sm
    from ...content.struct import legacy_classes as sh
    from ...utils.decorators import deprecated_with_alternative

Native = Union[TextFile, ColumnarMixin]
Struct = Union[StructInterface, Auto, None]
Stream = Union[ColumnarMixin, RegularStream]
Type = Optional[FieldType]
Message = Union[AutoName, str, Array]

CHUNK_SIZE = 8192
EXAMPLE_STR_LEN = 12
COUNT_ITEMS_TO_LOG_COLLECT_OPERATION = 500000
STREAM_META_FIELDS = ('count', )


class ColumnFile(LocalFile, ColumnarMixin):
    def __init__(
            self,
            name: str, gzip: bool = False, encoding: str = 'utf8',
            end: str = '\n', delimiter: str = '\t',
            first_line_is_title: bool = True,
            expected_count: AutoCount = AUTO,
            struct: Struct = AUTO,
            folder: Connector = None,
            verbose: AutoBool = AUTO,
    ):
        content_class = ContentType.ColumnFile.get_class()
        content_format = content_class(
            encoding=encoding, ending=end, delimiter=delimiter, first_line_is_title=first_line_is_title,
            compress='gzip' if gzip else None,
        )
        super().__init__(
            name=name, content_format=content_format, struct=struct,
            folder=folder, expected_count=expected_count, verbose=verbose,
        )

    def get_default_file_extension(self) -> str:
        return 'tsv'

    def get_item_type(self) -> ItemType:
        return ItemType.Record

    @staticmethod
    def get_default_item_type() -> ItemType:
        return ItemType.Row

    @classmethod
    def get_stream_type(cls) -> StreamType:
        return StreamType.RowStream

    def get_csv_reader(self, lines: Iterable) -> Iterator:
        if self.get_delimiter():
            return csv.reader(lines, delimiter=self.get_delimiter())
        else:
            return csv.reader(lines)

    def get_rows(
            self,
            convert_types: bool = True,
            verbose: AutoBool = AUTO,
            message: Message = AUTO,
            step: AutoCount = AUTO,
    ) -> Iterable:
        lines = self.get_lines(
            skip_first=self.is_first_line_title(),
            verbose=verbose, message=message, step=step,
        )
        rows = self.get_csv_reader(lines)
        if self.get_struct() is None or not convert_types:
            yield from rows
        else:
            converters = self.get_struct().get_converters('str', 'py')
            for row in rows:
                converted_row = list()
                for value, converter in zip(row, converters):
                    converted_value = converter(value)
                    converted_row.append(converted_value)
                yield converted_row

    def get_struct_rows(self, verbose: AutoBool = AUTO, message: Message = AUTO, step: AutoCount = AUTO) -> Iterable:
        assert self.get_struct() is not None, 'For getting structured rows struct must be defined.'
        for row in self.get_rows(verbose=verbose, message=message, step=step):
            yield sh.StructRow(row, struct=self.get_struct())

    def get_records_from_file(
            self,
            convert_types: bool = True,
            skip_missing: bool = False,
            verbose: AutoBool = AUTO,
            message: Message = AUTO,
            **kwargs
    ) -> Iterable:
        if self.is_existing():
            struct = self.get_struct()
            assert struct, 'Struct must be defined for {}.get_records_from_file()'.format(self.__repr__())
            columns = struct.get_columns()
            if self.get_count(allow_slow_gzip=False) <= 1:
                self.get_count(allow_slow_gzip=False, force=True)
            for item in self.get_rows(convert_types=convert_types, verbose=verbose, message=message, **kwargs):
                yield {k: v for k, v in zip(columns, item)}
        elif not skip_missing:
            raise FileNotFoundError('File {} is not existing'.format(self.get_name()))

    def get_records(self, convert_types: bool = True, verbose: AutoBool = AUTO, **kwargs) -> Iterable:
        return self.get_records_from_file(convert_types=convert_types, verbose=verbose, **kwargs)

    def select(self, *args, **kwargs) -> Stream:
        stream = self.to_record_stream().select(*args, **kwargs)
        return self._assume_stream(stream)

    def sort(self, *keys, reverse: bool = False) -> Stream:
        stream = self.to_stream()
        if hasattr(stream, 'sort'):
            return stream.sort(*keys, reverse=reverse)
        else:
            raise AttributeError('Stream {} has no attribute sort()'.format(stream))

    def to_memory(self) -> Stream:
        stream = self.to_record_stream().collect()
        return self._assume_stream(stream)

    def write_rows(self, rows: Iterable, verbose: AutoBool = AUTO) -> NoReturn:
        def get_rows_with_title():
            if self.is_first_line_title():
                yield self.get_columns()
            for r in rows:
                assert len(r) == len(self.get_columns())
                yield map(str, r)
        lines = map(self.delimiter.join, get_rows_with_title())
        self.write_lines(lines, verbose=verbose)

    def write_records(self, records: Iterable, verbose: AutoBool = AUTO) -> NoReturn:
        rows = map(
            lambda r: [r.get(f, '') for f in self.get_columns()],
            records,
        )
        self.write_rows(rows, verbose=verbose)


class CsvFile(LocalFile, ColumnarMixin):
    # @deprecated_with_alternative('ConnType.get_class()')
    def __init__(
            self,
            name: Name, struct: Struct = AUTO,
            gzip: bool = False, encoding: str = 'utf8',
            end: str = '\n', delimiter: str = ',',
            first_line_is_title: bool = True, expected_count: AutoBool = AUTO,
            folder: Connector = None, verbose: AutoBool = AUTO,
    ):
        content_class = ContentType.CsvFile.get_class()
        content_format = content_class(
            struct=struct, first_line_is_title=first_line_is_title,
            encoding=encoding, ending=end, delimiter=delimiter,
            compress='gzip' if gzip else None,
        )
        super().__init__(
            name=name, content_format=content_format, struct=struct,
            folder=folder, expected_count=expected_count, verbose=verbose,
        )

    def get_default_file_extension(self) -> str:
        return 'csv'

    @staticmethod
    def get_default_item_type() -> ItemType:
        return ItemType.Record

    @classmethod
    def get_stream_type(cls) -> StreamType:
        return StreamType.RecordStream

    def select(self, *args, **kwargs) -> Stream:
        stream = self.to_record_stream().select(*args, **kwargs)
        return self._assume_stream(stream)


class TsvFile(LocalFile, ColumnarMixin):
    # @deprecated_with_alternative('ConnType.get_class()')
    def __init__(
            self,
            name: Name, struct: Struct = AUTO,
            gzip: bool = False, encoding: str = 'utf8',
            end: str = '\n', delimiter: str = '\t',
            first_line_is_title: bool = True, expected_count: AutoBool = AUTO,
            folder: Connector = None, verbose: AutoBool = AUTO,
    ):
        content_class = ContentType.TsvFile.get_class()
        content_format = content_class(
            struct=struct, first_line_is_title=first_line_is_title,
            encoding=encoding, ending=end, delimiter=delimiter,
            compress='gzip' if gzip else None,
        )
        super().__init__(
            name=name, content_format=content_format, struct=struct,
            folder=folder, expected_count=expected_count, verbose=verbose,
        )

    def get_default_file_extension(self) -> str:
        return 'tsv'

    @staticmethod
    def get_default_item_type() -> ItemType:
        return ItemType.Record

    @classmethod
    def get_stream_type(cls) -> StreamType:
        return StreamType.RecordStream

    def select(self, *args, **kwargs) -> Stream:
        stream = self.to_record_stream().select(*args, **kwargs)
        return self._assume_stream(stream)


FileType.set_dict_classes(
    {
        FileType.TextFile: TextFile,
        FileType.JsonFile: JsonFile,
        FileType.ColumnFile: ColumnFile,
        FileType.CsvFile: CsvFile,
        FileType.TsvFile: TsvFile,
    }
)
ConnType.add_classes(
    TextFile,
    JsonFile,
    ColumnFile,
    CsvFile,
    TsvFile,
)
