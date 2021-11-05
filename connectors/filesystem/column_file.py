from typing import Optional, Union, Iterable, Iterator, NoReturn
import csv

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from interfaces import (
        StructInterface, Connector, LeafConnector, RegularStream, LineStream, RowStream, RecordStream, StructStream,
        FieldType, ItemType, StreamType, FileType, DialectType,
        Field, Name, Item, Array, Columns,
        AUTO, Auto, AutoName, AutoCount, AutoBool, OptionalFields
    )
    from connectors.filesystem.local_file import LocalFile, AbstractFormat, ContentType
    from connectors.filesystem.text_file import TextFile, JsonFile
    from streams.mixin.columnar_mixin import ColumnarMixin
    from streams import stream_classes as sm
    from fields import field_classes as fc
    from items import legacy_classes as sh
    from utils.decorators import deprecated_with_alternative
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...interfaces import (
        StructInterface, Connector, LeafConnector, RegularStream, LineStream, RowStream, RecordStream, StructStream,
        FieldType, ItemType, StreamType, FileType, DialectType,
        Field, Name, Item, Array, Columns,
        AUTO, Auto, AutoName, AutoCount, AutoBool, OptionalFields
    )
    from .local_file import LocalFile, AbstractFormat, ContentType
    from .text_file import TextFile, JsonFile
    from ...streams.mixin.columnar_mixin import ColumnarMixin
    from ...streams import stream_classes as sm
    from ...fields import field_classes as fc
    from ...items import legacy_classes as sh
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

    def get_struct_comparison_dict(self, other: Optional[Struct] = None) -> dict:
        if not arg.is_defined(other):
            other = self.get_initial_struct()
        return self.get_struct().get_struct_comperison_dict(other)

    def reset_struct_to_initial(self, verbose: bool = True, message: Optional[str] = None) -> Native:
        if not arg.is_defined(message):
            message = self.__repr__()
        if verbose:
            for line in self.get_struct().get_struct_comparison_iter(self.get_initial_struct(), message=message):
                self.log(line)
        return self.struct(self.get_initial_struct())

    def get_schema(self) -> Struct:
        return self.get_struct()

    def get_struct_str(self, dialect: DialectType = DialectType.Postgres) -> str:
        return self.get_struct().get_struct_str(dialect=dialect)

    def detect_struct_by_title_row(self) -> Native:
        struct = self.get_detected_struct_by_title_row()
        self.set_struct(struct, inplace=True)
        return self

    def get_detected_columns(self) -> StructInterface:
        return self.get_detected_struct_by_title_row(set_struct=False, verbose=False)

    def get_one_column_values(self, column: Field) -> Iterable:
        if isinstance(column, int):
            item_type = ItemType.Row
        elif isinstance(column, str):
            item_type = ItemType.Record
        else:
            raise ValueError('Expected column as int or str, got {}'.format(column))
        for item in self.get_items_of_type(item_type=item_type):
            yield item_type.get_value_from_item(item=item, field=column, skip_unsupported_types=True)

    def validate_fields(self, initial: bool = True) -> Native:
        if initial:
            expected_struct = self.get_initial_struct().copy()
        else:
            expected_struct = self.get_struct()
        actual_struct = self.get_detected_struct_by_title_row(set_struct=False, verbose=False)
        if not isinstance(actual_struct, fc.FlatStruct):
            actual_struct = fc.FlatStruct.convert_to_native(actual_struct)
        self.set_struct(actual_struct.validate_about(expected_struct), inplace=True)
        return self

    def get_validation_message(self) -> str:
        self.validate_fields()
        if self.is_valid_struct():
            message = 'file has {} rows, {} valid columns:'.format(
                self.get_count(allow_slow_gzip=False), self.get_column_count(),
            )
        else:
            valid_count = self.get_column_count() - self.get_invalid_fields_count()
            message = '[INVALID] file has {} rows, {} columns = {} valid + {} invalid:'.format(
                self.get_count(allow_slow_gzip=False), self.get_column_count(),
                valid_count, self.get_invalid_fields_count(),
            )
        if not hasattr(self.get_struct(), 'get_caption'):
            message = '[DEPRECATED] {}'.format(message)
        return message

    def get_invalid_columns(self) -> Iterable:
        if hasattr(self.get_struct(), 'get_fields'):
            for f in self.get_struct().get_fields():
                if hasattr(f, 'is_valid'):
                    if not f.is_valid():
                        yield f

    def get_invalid_fields_count(self) -> Optional[int]:
        count = 0
        for _ in self.get_invalid_columns():
            count += 1
        return count

    def is_valid_struct(self) -> bool:
        for _ in self.get_invalid_columns():
            return False
        return True

    def get_check(
            self, must_exists: bool = False,
            check_types: bool = False, check_order: bool = False,
            skip_errors: bool = False,
    ) -> bool:
        expected_struct = self.get_struct()
        if self.is_existing():
            received_struct = self.get_detected_struct_by_title_row(verbose=False)
            expected = expected_struct
            received = received_struct
            if not check_types:
                received = received.get_columns()
                expected = expected.get_columns()
            if not check_order:
                received = sorted(received)
                expected = sorted(expected)
            if received == expected:
                return True
            else:
                template = '{} actual fields does not meet expected fields: \nRECEIVED = {} \nEXPECTED = {}'
                msg = template.format(self.__repr__(), arg.get_names(received_struct), arg.get_names(expected_struct))
                self.log(msg, end='\n')
                if skip_errors:
                    return False
                else:
                    raise AssertionError(msg)
        else:
            if must_exists:
                message = 'For struct validation file {} must exists'.format(self.get_name())
            elif expected_struct:
                return True
            else:
                message = 'Struct for validation must be defined: {}'.format(self.get_name())
            self.log(message)
            if skip_errors:
                return False
            else:
                raise FileNotFoundError(message)

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

    def add_fields(self, *fields, default_type: Type = None, inplace: bool = False) -> Optional[Native]:
        self.get_struct().add_fields(*fields, default_type=default_type, inplace=True)
        if not inplace:
            return self

    def remove_fields(self, *fields, inplace=True) -> Optional[Native]:
        self.get_struct().remove_fields(*fields, inplace=True)
        if not inplace:
            return self

    def get_columns(self) -> list:
        return self.get_struct().get_columns()

    def get_types(self, dialect: DialectType = DialectType.String) -> Iterable:
        return self.get_struct().get_types(dialect)

    def set_types(self, dict_field_types: Optional[dict] = None, inplace: bool = False, **kwargs) -> Optional[Native]:
        self.get_struct().set_types(dict_field_types=dict_field_types, inplace=True, **kwargs)
        if not inplace:
            return self

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

    def get_str_description(self) -> str:
        if self.is_existing():
            rows_count = self.get_count(allow_slow_gzip=False)
            if rows_count:
                cols_count = self.get_column_count() or 0
                invalid_count = self.get_invalid_fields_count() or 0
                valid_count = cols_count - invalid_count
                message = '{} rows, {} columns = {} valid + {} invalid'
                return message.format(rows_count, cols_count, valid_count, invalid_count)
            else:
                message = 'empty file, expected {} columns: {}'
        else:
            message = 'file not exists, expected {} columns: {}'
        return message.format(self.get_column_count(), ', '.join(self.get_columns()))

    def has_title(self) -> bool:
        if self.is_first_line_title():
            if self.is_existing():
                return bool(self.get_count(allow_slow_gzip=False))
        return False

    def get_useful_props(self) -> dict:
        if self.is_existing():
            return dict(
                is_actual=self.is_actual(),
                is_valid=self.is_valid_struct(),
                has_title=self.is_first_line_title(),
                is_opened=self.is_opened(),
                is_empty=self.is_empty(),
                count=self.get_count(allow_slow_gzip=False),
                path=self.get_path(),
            )
        else:
            return dict(
                is_existing=self.is_existing(),
                path=self.get_path(),
            )

    @staticmethod
    def _format_args(*args, **kwargs) -> str:
        formatted_filters = list(args) + ['{}={}'.format(k, v) for k, v in kwargs.items()]
        return ', '.join(map(str, formatted_filters))

    def _prepare_examples(self, *filters, safe_filter: bool = True, **filter_kwargs) -> tuple:
        filters = filters or list()
        if filter_kwargs and safe_filter:
            filter_kwargs = {k: v for k, v in filter_kwargs.items() if k in self.get_columns()}
        verbose = self.is_gzip() or self.get_count(allow_slow_gzip=False) > COUNT_ITEMS_TO_LOG_COLLECT_OPERATION
        stream_example = self.filter(*filters or [], **filter_kwargs, verbose=verbose)
        item_example = stream_example.get_one_item()
        str_filters = self._format_args(*filters, **filter_kwargs)
        if item_example:
            if str_filters:
                message = 'Example with filters: {}'.format(str_filters)
            else:
                message = 'Example without any filters:'
        else:
            message = '[EXAMPLE_NOT_FOUND] Example with this filters not found: {}'.format(str_filters)
            stream_example = None
            item_example = self.get_one_item(ItemType.Record)
        if item_example:
            if EXAMPLE_STR_LEN:
                for k, v in item_example.items():
                    v = str(v)
                    if len(v) > EXAMPLE_STR_LEN:
                        item_example[k] = str(v)[:EXAMPLE_STR_LEN] + '..'
        else:
            item_example = dict()
            stream_example = None
            message = '[EMPTY_DATA] There are no valid records in stream_dataset {}'.format(self.__repr__())
        return item_example, stream_example, message

    def show(self, count: int = 10, filters: Columns = None, columns: Columns = AUTO, recount: bool = False, **kwargs):
        if recount:
            self.actualize()
        return self.to_record_stream().show(count=count, filters=filters or list(), columns=columns)

    def show_example(
            self, count: int = 10,
            example: Optional[Stream] = None,
            columns: Optional[Array] = None,
            comment: str = '',
    ):
        if not arg.is_defined(example):
            example = self.get_record_stream()
        stream_example = example.take(count).collect()
        if comment:
            self.log('')
            self.log(comment)
        if stream_example:
            example = stream_example.get_demo_example(columns=columns)
            is_dataframe = hasattr(example, 'shape')
            if is_dataframe:
                return example
            else:
                for line in example:
                    self.log(line)

    def describe(
            self, *filter_args,
            count: Optional[int] = 10, columns: Optional[Array] = None,
            as_dataframe: bool = False, show_header: bool = True, safe_filter: bool = True,
            **filter_kwargs
    ):
        if show_header:
            for line in self.get_str_headers():
                self.log(line)
        example_item, example_stream, example_comment = dict(), None, ''
        if self.is_existing():
            self.actualize()
            if self.is_empty():
                message = '[EMPTY] file is empty, expected {} columns:'.format(self.get_column_count())
            else:
                message = self.get_validation_message()
                # self.log('{} rows, {} columns:'.format(self.get_count(), self.get_column_count()))
                example_item, example_stream, example_comment = self._prepare_examples(
                    safe_filter=safe_filter, filters=filter_args, **filter_kwargs,
                )
        else:
            message = '[NOT_EXISTS] file is not created yet, expected {} columns:'.format(self.get_column_count())
        if show_header:
            self.log('{} {}'.format(self.get_datetime_str(), message))
            if self.get_invalid_fields_count():
                self.log('Invalid columns: {}'.format(self._format_args(*self.get_invalid_columns())))
            self.log('')
        struct = fc.FlatStruct.convert_to_native(self.get_struct())
        assert isinstance(struct, fc.FlatStruct), 'got {}'.format(struct)
        dataframe = struct.describe(
            as_dataframe=as_dataframe, example=example_item,
            logger=self.get_logger(), comment=example_comment,
        )
        if dataframe is not None:
            return dataframe
        if example_stream and count:
            return self.show_example(
                count=count, example=example_stream,
                columns=columns, comment=example_comment,
            )

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
