from typing import Optional, Union, Iterable, Iterator, NoReturn
import csv

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from interfaces import (
        SchemaInterface, Connector, LeafConnector, RegularStream, LineStream, RowStream, RecordStream, SchemaStream,
        FieldType, ItemType, StreamType,
        Field, Name, Item, Array, Columns,
        AUTO, Auto, AutoName, AutoCount, AutoBool, OptionalFields
    )
    from fields import field_classes as fc
    from connectors.filesystem.local_file import TextFile
    from streams.mixin.columnar_mixin import ColumnarMixin
    from streams import stream_classes as sm
    from items import legacy_classes as sh
    from utils.decorators import deprecated_with_alternative
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...interfaces import (
        SchemaInterface, Connector, LeafConnector, RegularStream, LineStream, RowStream, RecordStream, SchemaStream,
        FieldType, ItemType, StreamType,
        Field, Name, Item, Array, Columns,
        AUTO, Auto, AutoName, AutoCount, AutoBool, OptionalFields
    )
    from ...fields import field_classes as fc
    from .local_file import TextFile
    from ...streams.mixin.columnar_mixin import ColumnarMixin
    from ...streams import stream_classes as sm
    from ...items import legacy_classes as sh
    from ...utils.decorators import deprecated_with_alternative

Native = Union[TextFile, ColumnarMixin]
Schema = Union[SchemaInterface, Auto, None]
Stream = Union[ColumnarMixin, RegularStream]
Type = Optional[FieldType]
Message = Union[AutoName, str, Array]
Dialect = str

CHUNK_SIZE = 8192
EXAMPLE_STR_LEN = 12
STREAM_META_FIELDS = ('count', )


class ColumnFile(TextFile, ColumnarMixin):
    def __init__(
            self,
            name: str, gzip: bool = False, encoding: str = 'utf8',
            end: str = '\n', delimiter: str = '\t',
            first_line_is_title: bool = True,
            expected_count: AutoCount = AUTO,
            schema: Schema = AUTO,
            folder: Connector = None,
            verbose: AutoBool = AUTO,
    ):
        super().__init__(
            name=name, gzip=gzip, encoding=encoding,
            end=end, expected_count=expected_count,
            folder=folder, verbose=verbose,
        )
        self.delimiter = delimiter
        self.first_line_is_title = first_line_is_title
        self.schema = None
        self.set_schema(schema)

    @staticmethod
    def get_item_type() -> ItemType:
        return ItemType.Record

    def is_in_memory(self) -> bool:
        return False

    def get_data(self, verbose: AutoBool = AUTO, *args, **kwargs) -> Iterable:
        return self.get_items(verbose=verbose, *args, **kwargs)

    def sort(self, *keys, reverse: bool = False) -> Stream:
        stream = self.to_stream()
        if hasattr(stream, 'sort'):
            return stream.sort(*keys, reverse=reverse)
        else:
            raise AttributeError('Stream {} has no attribute sort()'.format(stream))

    def get_schema(self) -> Schema:
        return self.schema

    def get_schema_str(self, dialect: Dialect = 'pg') -> str:
        return self.get_schema().get_schema_str(dialect=dialect)

    def set_schema(self, schema: Schema, return_file: bool = True) -> TextFile:
        if schema is None:
            self.schema = None
        elif isinstance(schema, SchemaInterface):
            self.schema = schema
        elif isinstance(schema, (list, tuple)):
            self.log('Schema as list is deprecated, use FlatStruct(SchemaInterface) class instead', level=30)
            has_types_descriptions = [isinstance(f, (list, tuple)) for f in schema]
            if max(has_types_descriptions):
                self.schema = fc.FlatStruct(schema)
            else:
                self.schema = sh.detect_schema_by_title_row(schema)
        elif schema == AUTO:
            if self.first_line_is_title:
                self.schema = self.detect_schema_by_title_row()
            else:
                self.schema = None
        else:
            message = 'schema must be FlatStruct(SchemaInterface), got {}'.format(type(schema))
            raise TypeError(message)
        if return_file:
            return self

    def get_title_row(self, close: bool = True) -> tuple:
        lines = self.get_lines(skip_first=False, check=False, verbose=False)
        rows = self.get_csv_reader(lines)
        title_row = next(rows)
        if close:
            self.close()
        return title_row

    def detect_schema_by_title_row(self, set_schema: bool = False, verbose: AutoBool = AUTO) -> SchemaInterface:
        assert self.first_line_is_title, 'Can detect schema by title row only if first line is a title row'
        verbose = arg.acquire(verbose, self.verbose)
        title_row = self.get_title_row(close=True)
        schema = sh.detect_schema_by_title_row(title_row)
        message = 'Schema for {} detected by title row: {}'.format(self.get_name(), schema.get_schema_str(None))
        self.log(message, end='\n', verbose=verbose)
        if set_schema:
            self.schema = schema
        return schema

    def get_one_column_values(self, column: Field) -> Iterable:
        if isinstance(column, int):
            item_type = ItemType.Row
        elif isinstance(column, str):
            item_type = ItemType.Record
        else:
            raise ValueError('Expected column as int or str, got {}'.format(column))
        for item in self.get_items(item_type=item_type):
            yield item_type.get_value_from_item(item=item, field=column, skip_unsupported_types=True)

    def check(self, must_exists: bool = False, check_types: bool = False, check_order: bool = False) -> NoReturn:
        file_exists = self.is_existing()
        if must_exists:
            assert file_exists, 'file {} must exists'.format(self.get_name())
        expected_schema = self.get_schema()
        if file_exists:
            received_schema = self.detect_schema_by_title_row()
            expected = expected_schema
            received = received_schema
            if not check_types:
                received = received.get_columns()
                expected = expected.get_columns()
            if not check_order:
                received = sorted(received)
                expected = sorted(expected)
            assert received == expected, 'LocalFile({}).check(): received {} != expected {}'.format(
                self.get_name(), received_schema, expected_schema,
            )
        else:
            assert expected_schema, 'schema for {} must be defined'.format(self.get_name())

    def add_fields(self, *fields, default_type: Type = None, inplace: bool = False) -> Optional[Native]:
        self.get_schema().add_fields(*fields, default_type=default_type, inplace=True)
        if not inplace:
            return self

    def remove_fields(self, *fields, inplace=True) -> Optional[Native]:
        self.get_schema().remove_fields(*fields, inplace=True)
        if not inplace:
            return self

    def get_columns(self) -> list:
        return self.get_schema().get_columns()

    def get_types(self, dialect=arg.AUTO) -> Iterable:
        return self.get_schema().get_types(dialect)

    def set_types(self, dict_field_types: Optional[dict] = None, inplace: bool = False, **kwargs) -> Optional[Native]:
        self.get_schema().set_types(dict_field_types=dict_field_types, inplace=True, **kwargs)
        if not inplace:
            return self

    def get_delimiter(self) -> str:
        return self.delimiter

    def set_delimiter(self, delimiter: str, inplace: bool) -> Optional[Native]:
        if inplace:
            self.delimiter = delimiter
        else:
            return self.make_new(delimiter=delimiter)

    def is_first_line_title(self) -> bool:
        return self.first_line_is_title

    def get_csv_reader(self, lines) -> Iterator:
        if self.delimiter:
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
        if self.schema is None or not convert_types:
            yield from rows
        else:
            converters = self.get_schema().get_converters('str', 'py')
            for row in rows:
                converted_row = list()
                for value, converter in zip(row, converters):
                    converted_value = converter(value)
                    converted_row.append(converted_value)
                yield converted_row

    def get_items(self, item_type=ItemType.Auto, *args, **kwargs) -> Iterable:
        item_type = arg.acquire(item_type, self.get_default_item_type())
        item_type = ItemType(item_type)
        method_name = 'get_{}s'.format(item_type.get_name().lower())
        method_callable = self.__getattribute__(method_name)
        return method_callable(*args, **kwargs)

    def get_schema_rows(self, verbose: AutoBool = AUTO, message: Message = AUTO, step: AutoCount = AUTO) -> Iterable:
        assert self.get_schema() is not None, 'For getting schematized rows schema must be defined.'
        for row in self.get_rows(verbose=verbose, message=message, step=step):
            yield sh.SchemaRow(row, schema=self.schema)

    def get_records_from_file(
            self,
            convert_types: bool = True,
            verbose: AutoBool = AUTO,
            message: Message = AUTO,
            **kwargs
    ) -> Iterable:
        schema = self.get_schema()
        assert schema, 'Schema must be defined for {}'.format(self)
        columns = schema.get_columns()
        if self.get_count() <= 1:
            self.count_lines(reopen=True)
        for item in self.get_rows(convert_types=convert_types, verbose=verbose, message=message, **kwargs):
            yield {k: v for k, v in zip(columns, item)}

    def get_records(self, convert_types: bool = True, verbose: AutoBool = AUTO, **kwargs) -> Iterable:
        return self.get_records_from_file(convert_types=convert_types, verbose=verbose, **kwargs)

    def get_dict(self, key: Union[Field, Array], value: Union[Field, Array], skip_errors: bool = False) -> dict:
        stream = self.to_record_stream(check=False)
        return stream.get_dict(key, value, skip_errors=skip_errors)

    def stream(self, data=AUTO, stream_type: StreamType = AUTO, ex: OptionalFields = None, **kwargs) -> Stream:
        stream = self.to_stream(data, stream_type=stream_type, ex=ex, source=self, count=self.get_count())
        return stream

    def to_row_stream(
            self,
            name: AutoName = AUTO,
            convert_types: bool = True,
            verbose: AutoBool = AUTO,
            message: Message = AUTO,
            **kwargs
    ) -> RowStream:
        data = self.get_rows(convert_types=convert_types, verbose=verbose, message=message)
        stream = sm.RowStream(
            **self.get_stream_kwargs(data=data, name=name, **kwargs)
        )
        return stream

    def to_schema_stream(
            self, name: AutoName = AUTO,
            verbose: AutoBool = AUTO, message: Message = AUTO,
            **kwargs
    ) -> SchemaStream:
        data = self.get_rows(convert_types=True, verbose=verbose, message=message)
        stream = sm.SchemaStream(
            schema=self.schema,
            **self.get_stream_kwargs(data=data, name=name, **kwargs)
        )
        return stream

    def to_record_stream(self, name: AutoName = AUTO, message: Message = AUTO, **kwargs) -> RecordStream:
        data = self.get_records_from_file(verbose=name, message=message)
        kwargs = self.get_stream_kwargs(data=data, name=name, **kwargs)
        kwargs['stream_type'] = sm.RecordStream
        return self.stream(**kwargs)

    def progress(self, step: AutoCount = AUTO, message: Message = AUTO) -> Stream:
        if '{}' in message:
            message.format(self.get_name())
        return self.to_record_stream().progress(step=step, expected_count=self.get_count(True), message=message)

    def get_one_item(self, item_type: Union[ItemType, str] = ItemType.Record) -> Item:
        type_name = item_type if isinstance(item_type, str) else item_type.get_name().lower()
        method_name = 'to_{}_stream'.format(type_name)
        method = self.__getattribute__(method_name)
        return method().get_one_item()

    def select(self, *args, **kwargs) -> Stream:
        stream = self.to_record_stream().select(*args, **kwargs)
        return self._assume_stream(stream)

    def filter(self, *args, **kwargs) -> Union[Stream, Native]:
        if args or kwargs:
            stream = self.to_record_stream().filter(*args, **kwargs)
            return self._assume_stream(stream)
        else:
            return self

    def take(self, count: Union[int, bool]) -> Stream:
        stream = self.to_record_stream()
        if count and isinstance(count, bool):
            return stream
        else:
            stream = stream.take(count)
            return self._assume_stream(stream)

    def to_memory(self) -> Stream:
        stream = self.to_record_stream().collect()
        return self._assume_stream(stream)

    def _prepare_examples(self, *filters, safe_filter: bool = True, **filter_kwargs) -> tuple:
        if filter_kwargs and safe_filter:
            filter_kwargs = {k: v for k, v in filter_kwargs.items() if k in self.get_columns()}
        stream_example = self.filter(*filters or [], **filter_kwargs)
        example = stream_example.one()
        if not example:
            message = 'Example record with this filters not found: {} {}'.format(filters or '', filter_kwargs or '')
            self.log(message)
            stream_example = self
            example = self.get_one_item(ItemType.Record)
        if example:
            if EXAMPLE_STR_LEN:
                for k, v in example.items():
                    v = str(v)
                    if len(v) > EXAMPLE_STR_LEN:
                        example[k] = str(v)[:EXAMPLE_STR_LEN] + '..'
        else:
            example = dict()
            stream_example = None
        return example, stream_example

    def show(self, count: int = 10, filters: Columns = None, columns: Columns = None, as_dataframe: AutoBool = AUTO):
        self.actualize()
        return self.to_record_stream().show(count=count, filters=filters or list(), columns=columns)

    def describe(
            self,
            example_count: Optional[int] = 10,
            example_columns: Optional[Array] = None,
            as_dataframe: bool = False,
            safe_filter: bool = True,
            filters: Optional[Array] = None,
            **filter_kwargs
    ):
        self.log(str(self))
        if (self.get_count() or 0) <= 1:
            self.count_lines(True)
        file_is_empty = (self.get_count() or 0) <= (1 if self.is_first_line_title() else 0)
        example, stream_example = dict(), None
        if self.is_existing():
            if file_is_empty:
                self.log('File is empty')
            else:
                self.log('{} rows, {} columns:'.format(self.get_count(), self.get_column_count()))
                example, stream_example = self._prepare_examples(
                    safe_filter=safe_filter, filters=filters, **filter_kwargs,
                )
        else:
            self.log('File is not created yet, expected {} columns:'.format(self.get_column_count()))
        struct = self.get_schema()
        if hasattr(struct, 'describe'):
            if as_dataframe and hasattr(struct, 'get_dataframe'):
                return struct.get_dataframe()
            else:
                struct.describe(logger=self.get_logger(), example=example)
        if stream_example and example_count:
            return self.get_demo_example(count=example_count, columns=example_columns)

    def write_rows(self, rows: Iterable, verbose: AutoBool = AUTO) -> NoReturn:
        def get_rows_with_title():
            if self.first_line_is_title:
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

    def write_stream(self, stream: Stream, verbose: AutoBool = AUTO) -> NoReturn:
        assert sm.is_stream(stream)
        item_type_str = stream.get_item_type().get_value()
        if item_type_str in ('row', 'record', 'row'):
            method_name = 'write_{}s'.format(item_type_str)
            method = self.__getattribute__(method_name)
            return method(stream.get_data(), verbose=verbose)
        else:
            message = '{}.write_stream() supports RecordStream, RowStream, LineStream only (got {})'
            raise TypeError(message.format(self.__class__.__name__, stream.__class__.__name__))

    def is_empty(self) -> bool:
        return (self.get_columns() or 0) <= (1 if self.is_first_line_title() else 0)

    @staticmethod
    def get_default_file_extension() -> str:
        return 'tsv'

    @staticmethod
    def get_default_item_type() -> ItemType:
        return ItemType.Row

    @classmethod
    def get_stream_type(cls) -> StreamType:
        return StreamType.RowStream

    @staticmethod
    def _assume_stream(stream) -> Native:
        return stream


class CsvFile(ColumnFile):
    # @deprecated_with_alternative('ConnType.get_class()')
    def __init__(
            self,
            name,
            gzip=False,
            encoding='utf8',
            end='\n',
            delimiter=',',
            first_line_is_title=True,
            expected_count=AUTO,
            schema=AUTO,
            folder=None,
            verbose=AUTO
    ):
        super().__init__(
            name=name,
            gzip=gzip,
            encoding=encoding,
            end=end,
            expected_count=expected_count,
            folder=folder,
            verbose=verbose,
        )
        self.delimiter = delimiter
        self.first_line_is_title = first_line_is_title
        self.schema = None
        self.set_schema(schema)

    @staticmethod
    def get_default_file_extension() -> str:
        return 'csv'

    @staticmethod
    def get_default_item_type() -> ItemType:
        return ItemType.Row

    @classmethod
    def get_stream_type(cls) -> StreamType:
        return StreamType.RecordStream


# @deprecated_with_alternative('ConnType.get_class()')
class TsvFile(ColumnFile):
    def __init__(
            self,
            name,
            gzip=False,
            encoding='utf8',
            end='\n',
            delimiter='\t',
            first_line_is_title=True,
            expected_count=AUTO,
            schema=AUTO,
            folder=None,
            verbose=AUTO
    ):
        super().__init__(
            name=name,
            gzip=gzip,
            encoding=encoding,
            end=end,
            delimiter=delimiter,
            first_line_is_title=first_line_is_title,
            expected_count=expected_count,
            schema=schema,
            folder=folder,
            verbose=verbose,
        )

    @staticmethod
    def get_default_file_extension() -> str:
        return 'tsv'

    @staticmethod
    def get_default_item_type() -> ItemType:
        return ItemType.Record

    @classmethod
    def get_stream_type(cls) -> StreamType:
        return StreamType.RecordStream
