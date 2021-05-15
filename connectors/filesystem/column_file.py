from typing import Optional, Union, Any, Iterable, NoReturn
import csv

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from items import base_item_type as it
    from fields import field_classes as fc
    from connectors.filesystem.local_file import TextFile
    from streams.mixin.columnar_mixin import ColumnarMixin
    from streams import stream_classes as sm
    from schema import schema_classes as sh
    from utils.decorators import deprecated_with_alternative
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...items import base_item_type as it
    from ...fields import field_classes as fc
    from .local_file import TextFile
    from ...streams.mixin.columnar_mixin import ColumnarMixin
    from ...streams import stream_classes as sm
    from ...schema import schema_classes as sh
    from ...utils.decorators import deprecated_with_alternative

OptionalFields = Optional[Union[str, Iterable]]
Stream = Union[ColumnarMixin, Any]

AUTO = arg.DEFAULT
CHUNK_SIZE = 8192
STREAM_META_FIELDS = ('count', )


class ColumnFile(TextFile, ColumnarMixin):
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
            expected_count=expected_count,
            folder=folder,
            verbose=verbose,
        )
        self.delimiter = delimiter
        self.first_line_is_title = first_line_is_title
        self.schema = None
        self.set_schema(schema)

    @staticmethod
    def get_item_type() -> it.ItemType:
        return it.ItemType.Record

    def is_in_memory(self) -> bool:
        return False

    def get_data(self, verbose=arg.DEFAULT, *args, **kwargs) -> Iterable:  # ?
        return self.get_items(verbose=verbose, *args, **kwargs)

    def sort(self, *keys, reverse=False):
        raise NotImplemented

    def get_schema(self) -> sh.SchemaInterface:
        return self.schema

    def get_schema_str(self, dialect='pg') -> str:
        return self.get_schema().get_schema_str(dialect=dialect)

    def set_schema(self, schema, return_file=True) -> TextFile:
        if schema is None:
            self.schema = None
        elif isinstance(schema, sh.SchemaInterface):
            self.schema = schema
        elif isinstance(schema, (list, tuple)):
            self.log('Schema as list is deprecated, use FieldGroup(SchemaInterface) class instead', level=30)
            has_types_descriptions = [isinstance(f, (list, tuple)) for f in schema]
            if max(has_types_descriptions):
                self.schema = fc.FieldGroup(schema)
            else:
                self.schema = sh.detect_schema_by_title_row(schema)
        elif schema == AUTO:
            if self.first_line_is_title:
                self.schema = self.detect_schema_by_title_row()
            else:
                self.schema = None
        else:
            message = 'schema must be FieldGroup(SchemaInterface), got {}'.format(type(schema))
            raise TypeError(message)
        if return_file:
            return self

    def get_title_row(self, close=True):
        lines = self.get_lines(skip_first=False, check=False, verbose=False)
        rows = csv.reader(lines, delimiter=self.delimiter) if self.delimiter else csv.reader(lines)
        title_row = next(rows)
        if close:
            self.close()
        return title_row

    def detect_schema_by_title_row(self, set_schema=False, verbose=AUTO) -> sh.SchemaInterface:
        assert self.first_line_is_title, 'Can detect schema by title row only if first line is a title row'
        verbose = arg.undefault(verbose, self.verbose)
        title_row = self.get_title_row(close=True)
        schema = sh.detect_schema_by_title_row(title_row)
        message = 'Schema for {} detected by title row: {}'.format(self.get_name(), schema.get_schema_str(None))
        self.log(message, end='\n', verbose=verbose)
        if set_schema:
            self.schema = schema
        return schema

    def get_one_column_values(self, column: Union[int, str]) -> Iterable:
        if isinstance(column, int):
            item_type = it.ItemType.Row
        elif isinstance(column, str):
            item_type = it.ItemType.Record
        else:
            raise ValueError
        for item in self.get_items(item_type=item_type):
            yield item_type.get_value_from_item(item=item, field=column, skip_unsupported_types=True)

    def check(self, must_exists=False, check_types=False, check_order=False) -> NoReturn:
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

    def add_fields(self, *fields, default_type=None, return_file=True) -> TextFile:
        self.schema.add_fields(*fields, default_type=default_type, inplace=True)
        if return_file:
            return self

    def get_columns(self) -> list:
        return self.get_schema().get_columns()

    def get_types(self, dialect=arg.DEFAULT) -> Iterable:
        return self.get_schema().get_types(dialect)

    def set_types(self, dict_field_types: Optional[dict] = None, inplace: bool = False, **kwargs):
        self.get_schema().set_types(dict_field_types=dict_field_types, inplace=True, **kwargs)
        if not inplace:
            return self

    def get_rows(self, convert_types=True, verbose=AUTO, step=AUTO) -> Iterable:
        lines = self.get_lines(
            skip_first=self.first_line_is_title,
            verbose=verbose, step=step,
        )
        rows = csv.reader(lines, delimiter=self.delimiter) if self.delimiter else csv.reader(lines)
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

    def get_items(self, item_type=it.ItemType.Auto, *args, **kwargs) -> Iterable:
        item_type = arg.undefault(item_type, self.get_default_item_type())
        item_type = it.ItemType(item_type)
        method_name = 'get_{}s'.format(item_type.get_name())
        method_callable = self.__getattribute__(method_name)
        return method_callable(*args, **kwargs)

    def get_schema_rows(self, verbose=AUTO, step=AUTO) -> Iterable:
        assert self.schema is not None, 'For getting schematized rows schema must be defined.'
        for row in self.get_rows(verbose=verbose, step=step):
            yield sh.SchemaRow(row, schema=self.schema)

    def get_records_from_file(self, convert_types=True, verbose=arg.DEFAULT, **kwargs) -> Iterable:
        schema = self.get_schema()
        assert schema, 'Schema must be defined for {}'.format(self)
        columns = schema.get_columns()
        for item in self.get_rows(convert_types=convert_types, verbose=verbose, **kwargs):
            yield {k: v for k, v in zip(columns, item)}

    def get_records(self, convert_types=True, verbose=arg.DEFAULT, **kwargs) -> Iterable:
        return self.get_records_from_file(convert_types=convert_types, verbose=verbose, **kwargs)

    def get_dict(self, key, value, skip_errors=False) -> dict:
        stream = self.to_record_stream(check=False)
        return stream.get_dict(key, value, skip_errors=skip_errors)

    def stream(self, data=AUTO, stream_type=AUTO, ex: OptionalFields = None, **kwargs) -> Stream:
        stream = self.to_stream(data, stream_type=stream_type, ex=ex, source=self, count=self.get_count())
        return stream

    def to_row_stream(self, name=AUTO, **kwargs) -> Stream:
        data = self.get_rows()
        stream = sm.RowStream(
            **self.get_stream_kwargs(data=data, name=name, **kwargs)
        )
        return stream

    def to_schema_stream(self, name=AUTO, **kwargs) -> Stream:
        data = self.get_rows()
        stream = sm.SchemaStream(
            schema=self.schema,
            **self.get_stream_kwargs(data=data, name=name, **kwargs)
        )
        return stream

    def to_record_stream(self, name=arg.DEFAULT, **kwargs) -> Stream:
        data = self.get_records_from_file(verbose=name)
        kwargs = self.get_stream_kwargs(data=data, name=name, **kwargs)
        kwargs['stream_type'] = sm.RecordStream
        return self.stream(**kwargs)

    def select(self, *args, **kwargs) -> Stream:
        return self.to_record_stream().select(*args, **kwargs)

    def filter(self, *args, **kwargs) -> Stream:
        return self.to_record_stream().filter(*args, **kwargs)

    def take(self, count) -> Stream:
        return self.to_record_stream().take(count)

    def to_memory(self) -> Stream:
        return self.to_record_stream().to_memory()

    def show(self, count: int = 10, filters: Optional[list] = None, recount=False):
        if recount:
            self.count_lines(True)
        return self.to_record_stream().show(count=count, filters=filters or list())

    def write_rows(self, rows, verbose=AUTO) -> NoReturn:
        def get_rows_with_title():
            if self.first_line_is_title:
                yield self.get_columns()
            for r in rows:
                assert len(r) == len(self.get_columns())
                yield map(str, r)
        lines = map(self.delimiter.join, get_rows_with_title())
        self.write_lines(lines, verbose=verbose)

    def write_records(self, records, verbose=AUTO) -> NoReturn:
        rows = map(
            lambda r: [r.get(f, '') for f in self.get_columns()],
            records,
        )
        self.write_rows(rows, verbose=verbose)

    def write_stream(self, stream: Stream, verbose=AUTO) -> NoReturn:
        assert sm.is_stream(stream)
        item_type_str = stream.get_item_type().get_value()
        if item_type_str in ('row', 'record', 'row'):
            method_name = 'write_{}s'.format(item_type_str)
            method = self.__getattribute__(method_name)
            return method(stream.get_data(), verbose=verbose)
        else:
            message = '{}.write_stream() supports RecordStream, RowStream, LineStream only (got {})'
            raise TypeError(message.format(self.__class__.__name__, stream.__class__.__name__))

    @staticmethod
    def get_default_file_extension() -> str:
        return 'tsv'

    @staticmethod
    def get_default_item_type() -> it.ItemType:
        return it.ItemType.Row

    @classmethod
    def get_stream_type(cls):
        return sm.StreamType.RowStream


# @deprecated_with_alternative('ConnType.get_class()')
class CsvFile(ColumnFile):
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
    def get_default_item_type() -> it.ItemType:
        return it.ItemType.Row

    @classmethod
    def get_stream_type(cls):
        return sm.StreamType.RecordStream


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
    def get_default_item_type() -> it.ItemType:
        return it.ItemType.Record

    @classmethod
    def get_stream_type(cls):
        return sm.StreamType.RecordStream
