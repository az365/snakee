from typing import Optional, Iterable, Iterator, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        ConnectorInterface, StructInterface, ColumnarInterface, RegularStream, ExtendedLoggerInterface,
        ContentFormatInterface, ContentType, ConnType, ItemType, StreamType, LoggingLevel,
        ARRAY_TYPES, Array, Name, Count, OptionalFields, Links,
        AUTO, Auto, AutoBool, AutoName, AutoCount, AutoLinks, AutoContext,
    )
    from base.functions.arguments import update, get_str_from_args_kwargs
    from content.struct.flat_struct import FlatStruct
    from connectors.abstract.leaf_connector import LeafConnector
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        ConnectorInterface, StructInterface, ColumnarInterface, RegularStream, ExtendedLoggerInterface,
        ContentFormatInterface, ContentType, ConnType, ItemType, StreamType, LoggingLevel,
        ARRAY_TYPES, Array, Name, Count, OptionalFields, Links,
        AUTO, Auto, AutoBool, AutoName, AutoCount, AutoLinks, AutoContext,
    )
    from ...base.functions.arguments import update, get_str_from_args_kwargs
    from ...content.struct.flat_struct import FlatStruct
    from ..abstract.leaf_connector import LeafConnector

Native = LeafConnector
Stream = Union[RegularStream, ColumnarInterface]
GeneralizedStruct = Union[StructInterface, list, tuple, Auto, None]

META_MEMBER_MAPPING = dict(_source='database')
MAX_ITEMS_IN_MEMORY = 10000
EXAMPLE_ROW_COUNT = 10
EXAMPLE_STR_LEN = 12
CONTINUE_SYMBOL = '..'


class Table(LeafConnector):
    def __init__(
            self,
            name: Name,
            database: ConnectorInterface,
            content_format: Union[ContentFormatInterface, Auto] = AUTO,
            struct: Union[StructInterface, Auto] = AUTO,
            caption: Optional[str] = None,
            streams: Links = None,
            context: AutoContext = AUTO,
            reconnect: bool = False,
            expected_count: AutoCount = AUTO,
            verbose: AutoBool = AUTO,
    ):
        super().__init__(
            name=name,
            content_format=content_format,
            struct=struct,
            caption=caption,
            parent=database,
            context=context,
            streams=streams,
            expected_count=expected_count,
            verbose=verbose,
        )
        if reconnect and hasattr(database, 'connect'):
            database.connect(reconnect=True)

    def get_conn_type(self) -> ConnType:
        return ConnType.Table

    def get_content_type(self) -> ContentType:
        return ContentType.TsvFile

    @classmethod
    def _get_meta_member_mapping(cls) -> dict:
        meta_member_mapping = super()._get_meta_member_mapping()
        meta_member_mapping.update(META_MEMBER_MAPPING)
        return meta_member_mapping

    @staticmethod
    def _get_detected_format_by_name(name: str, **kwargs) -> ContentFormatInterface:
        content_type = ContentType.TsvFile
        if kwargs:
            content_class = content_type.get_class()
            try:
                return content_class(**kwargs)
            except TypeError as e:
                raise TypeError('{}: {}'.format(content_class.__name__, e))
        else:
            return content_type

    def _get_detected_struct(
            self,
            set_struct: bool = False,
            use_declared_types: AutoBool = AUTO,  # ?
            skip_missing: bool = False,
            verbose: AutoBool = AUTO,
    ) -> GeneralizedStruct:
        struct = self.get_struct_from_database(set_struct=set_struct)
        if struct:
            if not isinstance(struct, StructInterface) and Auto.delayed_acquire(verbose, self.is_verbose):
                message = 'Struct as {} is deprecated. Use items.FlatStruct instead.'.format(type(struct))
                self.log(msg=message, level=LoggingLevel.Warning)
        elif not skip_missing:
            raise ValueError(f'Received empty struct from {self}')
        return struct

    def get_database(self) -> ConnectorInterface:
        database = self.get_parent()
        return self._assume_connector(database)

    def is_opened(self) -> Optional[bool]:
        database = self.get_database()
        if hasattr(database, 'is_connected'):  # isinstance(database, PostgresDatabase)
            return database.is_connected()
        elif hasattr(database, 'is_opened'):
            return database.is_opened()

    def is_actual(self) -> bool:
        return Auto.is_defined(self.get_expected_count()) and self.get_initial_struct()

    def get_modification_timestamp(self, reset: bool = True) -> Optional[float]:
        if self.is_actual():
            timestamp = self.get_prev_modification_timestamp()
            if not timestamp:
                timestamp = self._get_current_timestamp()
        elif self.is_existing():
            timestamp = self._get_current_timestamp()
        else:
            timestamp = None
        if reset:
            self.reset_modification_timestamp(timestamp)
        return timestamp

    def get_count(self, allow_slow_mode: bool = True, allow_reopen: bool = True, force: bool = False) -> Count:
        if force:
            must_recount = True
            count = None
        else:
            count = self.get_expected_count()
            if Auto.is_defined(count):
                must_recount = False
            elif allow_slow_mode:
                must_recount = self.is_existing()
            else:
                must_recount = False
        if must_recount:
            count = self.get_actual_lines_count(allow_slow_mode=allow_slow_mode)
            self.set_count(count)
        if Auto.is_defined(count):
            return count

    def get_actual_lines_count(
            self,
            allow_slow_mode: bool = False,
            verbose: AutoBool = AUTO,
    ) -> Count:
        if allow_slow_mode:
            database = self.get_database()
            count = database.select_count(self.get_name(), verbose=verbose)
        else:
            count = self.get_expected_count()
        return count

    def get_columns(self, skip_missing: bool = False) -> Optional[list]:
        struct = self.get_struct()
        if struct:
            return struct.get_columns()
        elif skip_missing:
            return None
        else:
            name = self.get_name()
            raise ValueError(f'Table.get_columns(skip_missing=False): Struct for {name} is not defined: {self}.')

    def set_struct(self, struct: GeneralizedStruct, inplace: bool) -> Optional[Native]:
        if isinstance(struct, StructInterface) or struct is None:
            pass
        elif isinstance(struct, ARRAY_TYPES):
            if max([isinstance(f, ARRAY_TYPES) for f in struct]):
                struct = FlatStruct(struct)
            else:
                struct = FlatStruct.get_struct_detected_by_title_row(struct)
        elif struct == AUTO:
            struct = self._get_struct_from_source()
        else:
            message = 'struct must be StructInterface or tuple with fields_description (got {})'.format(type(struct))
            raise TypeError(message)
        return super().set_struct(struct, inplace=inplace)

    def get_struct_from_database(
            self,
            types: AutoLinks = AUTO,
            set_struct: bool = False,
            skip_missing: bool = False,
            verbose: AutoBool = AUTO,
    ) -> StructInterface:
        struct = FlatStruct(self.describe_table(verbose=verbose))
        if struct.is_empty() and not skip_missing:
            raise ValueError('Can not get struct for non-existing table {}'.format(self))
        if Auto.is_defined(types):
            struct.set_types(types, inplace=True)
        if set_struct:
            self.set_struct(struct, inplace=True)
        return struct

    def _get_struct_from_source(
            self,
            types: Union[dict, Auto, None] = AUTO,
            skip_missing: bool = False,
            verbose: bool = False,
    ) -> GeneralizedStruct:
        return self.get_struct_from_database(types=types, skip_missing=skip_missing, verbose=verbose)

    def get_first_line(self, close: bool = True, skip_missing: bool = False, verbose: bool = True) -> Optional[str]:
        if skip_missing:
            if not self.is_existing():
                return None
        iter_lines = self.execute_select(fields='*', count=1, verbose=verbose)
        lines = list(iter_lines)
        if close:
            self.close()
        if lines:
            return lines[0]

    def take(self, count: Union[int, bool] = 1, inplace: bool = False) -> Stream:
        iter_lines = self.execute_select(fields='*', count=count, verbose=self.is_verbose())
        return self.stream(iter_lines, count=count)

    def get_lines(
            self,
            count: Optional[int] = None,
            skip_first: bool = False,
            skip_missing: bool = False,
            allow_reopen: bool = True,
            verbose: AutoBool = AUTO,
            message: AutoName = AUTO,
            step: AutoCount = AUTO,
    ) -> Iterator[str]:
        if skip_missing:
            if not self.is_existing():
                yield from []
        yield from self.execute_select(fields='*', count=count, verbose=verbose)
        self.close()

    def get_rows(self, verbose: AutoBool = AUTO) -> Iterable:
        database = self.get_database()
        return database.select_all(self.get_name(), verbose=verbose)

    def get_data(self, verbose: AutoBool = AUTO) -> Iterable:
        return self.get_rows(verbose=verbose)

    def get_items(self, verbose: AutoBool = AUTO) -> Iterable:
        return self.get_rows(verbose=verbose)

    def get_items_of_type(
            self,
            item_type: Union[ItemType, Auto],
            verbose: AutoBool = AUTO,
            message: AutoName = AUTO,
            step: AutoCount = AUTO,
    ) -> Iterable:
        item_type = Auto.delayed_acquire(item_type, self.get_item_type)
        rows = self.get_rows(verbose=verbose)
        if item_type == ItemType.Row:
            items = rows
        elif item_type == ItemType.StructRow:
            row_class = ItemType.StructRow.get_class()
            items = map(lambda i: row_class(i, self.get_struct()), rows)
        elif item_type == ItemType.Record:
            items = map(lambda r: {c: v for c, v in zip(self.get_columns(), r)}, rows)
        elif item_type == ItemType.Line:
            items = map(lambda r: '\t'.join([str(v) for v in r]), rows)
        else:
            raise ValueError('Table.get_items_of_type(): cannot convert Rows to {}'.format(item_type))
        if step:
            logger = self.get_logger()
            if isinstance(logger, ExtendedLoggerInterface):
                count = self._get_fast_count()
                if not Auto.is_defined(message):
                    message = 'Downloading {count} lines from {name}'
                if '{}' in message:
                    message = message.format(count, self.get_name())
                if '{' in message:
                    message = message.format(count=count, name=self.get_name())
                items = logger.progress(items, name=message, count=count, step=step, context=self.get_context())
        return items

    def from_stream(self, stream, **kwargs):
        assert isinstance(stream, RegularStream)
        self.upload(data=stream, **kwargs)

    def is_existing(self, verbose: AutoBool = AUTO) -> bool:
        database = self.get_database()
        return database.exists_table(self.get_path(), verbose=verbose)

    def describe_table(self, verbose: AutoBool = Auto) -> Iterable:
        database = self.get_database()
        return database.describe_table(self.get_path(), verbose=verbose)

    def create(self, drop_if_exists: bool, verbose: AutoBool = AUTO):
        database = self.get_database()
        return database.create_table(
            self.get_name(),
            struct=self.get_struct(),
            drop_if_exists=drop_if_exists,
            verbose=verbose,
        )

    def upload(
            self,
            data: Union[Iterable, Stream],
            encoding: Optional[str] = None,
            skip_first_line: bool = False,
            skip_lines: int = 0,
            max_error_rate: float = 0.0,
            verbose: AutoBool = AUTO,
    ):
        database = self.get_database()
        return database.safe_upload_table(
            self.get_name(),
            data=data,
            struct=self.get_struct(),
            skip_lines=skip_lines,
            skip_first_line=skip_first_line,
            encoding=encoding,
            max_error_rate=max_error_rate,
            verbose=verbose,
        )

    def is_empty(self) -> bool:
        count = self.get_count()
        return not count

    def to_stream(
            self,
            data: Union[Iterable, Auto] = AUTO,
            name: AutoName = AUTO,
            stream_type: Union[StreamType, Auto] = AUTO,
            ex: OptionalFields = None,
            step: AutoCount = AUTO,
            **kwargs
    ) -> Stream:  # SqlStream
        stream_type = Auto.acquire(stream_type, StreamType.SqlStream)
        if stream_type == StreamType.SqlStream:
            assert not Auto.is_defined(data)
            name = Auto.delayed_acquire(name, self._get_generated_stream_name)
            stream_class = stream_type.get_class()
            meta = self.get_compatible_meta(stream_class, name=name, ex=ex, **kwargs)
            meta['source'] = self
            return stream_class(data, **meta)
        else:
            return super().to_stream(
                data=data, name=name,
                stream_type=stream_type,
                ex=ex, step=step, **kwargs,
            )

    def execute_select(
            self,
            fields: OptionalFields,
            filters: OptionalFields = None,
            sort: OptionalFields = None,
            count: Count = None,
            verbose: AutoBool = AUTO,
    ) -> Iterable:
        database = self.get_database()
        rows = database.execute_select(
            table=self, verbose=verbose,
            fields=fields, filters=filters, sort=sort, count=count,
        )
        return rows

    def simple_select(
            self,
            fields: OptionalFields,
            filters: OptionalFields = None,
            sort: OptionalFields = None,
            count: Count = None,
            stream_type: Union[StreamType, Auto] = AUTO,
            verbose: AutoBool = AUTO,
    ) -> Stream:
        stream_type = Auto.acquire(stream_type, StreamType.RecordStream)
        stream_class = stream_type.get_class()
        stream_rows = self.execute_select(fields=fields, filters=filters, sort=sort, count=count, verbose=verbose)
        if stream_type == StreamType.RowStream:
            stream_data = stream_rows
        elif stream_type == StreamType.RecordStream:
            columns = self.get_columns()
            stream_data = map(lambda r: dict(zip(columns, r)), stream_rows)
        else:
            raise NotImplementedError
        if Auto.is_defined(count):
            if count < MAX_ITEMS_IN_MEMORY:
                stream_data = list(stream_data)
                count = len(stream_data)
        return stream_class(stream_data, count=count, source=self, context=self.get_context())

    def select(self, *columns, **expressions) -> Stream:
        stream = self.to_stream().select(*columns, **expressions)
        return self._assume_stream(stream)

    def filter(self, *columns, **expressions) -> Stream:
        stream = self.to_stream().filter(*columns, **expressions)
        return self._assume_stream(stream)

    def _prepare_examples(
            self,
            *filters,
            safe_filter: bool = True,
            example_row_count: Optional[int] = EXAMPLE_ROW_COUNT,
            example_str_len: int = EXAMPLE_STR_LEN,
            **filter_kwargs,
    ) -> tuple:
        filters = filters or list()
        if filter_kwargs and safe_filter:
            filter_kwargs = {k: v for k, v in filter_kwargs.items() if k in self.get_columns()}
        if filter_kwargs:
            stream_example = self.filter(*filters or [], **filter_kwargs).take(example_row_count)
        else:
            stream_example = self.simple_select(fields='*', filters=filters, count=example_row_count)
        str_filters = get_str_from_args_kwargs(*filters, **filter_kwargs)
        item_example = stream_example.get_one_item()
        if item_example:
            if str_filters:
                message = 'Example with filters: {}'.format(str_filters)
            else:
                message = 'Example without any filters:'
        else:
            message = '[EXAMPLE_NOT_FOUND] Example with this filters not found: {}'.format(str_filters)
            stream_example = None
            item_example = self.get_one_item()
        if item_example:
            if example_str_len:
                for k, v in item_example.items():
                    v = str(v)
                    if len(v) > example_str_len:
                        fixed_len = example_str_len - len(CONTINUE_SYMBOL)
                        if fixed_len < 0:
                            fixed_len = 0
                            continue_symbol = CONTINUE_SYMBOL[:example_str_len]
                        else:
                            continue_symbol = CONTINUE_SYMBOL
                        item_example[k] = str(v)[:fixed_len] + continue_symbol
        else:
            item_example = dict()
            stream_example = None
            message = '[EMPTY_DATA] There are no valid data in {}'.format(self.__repr__())
        return item_example, stream_example, message


ConnType.add_classes(Table)
