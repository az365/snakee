from typing import Optional, Iterable, Iterator, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        ConnectorInterface, StructInterface, ColumnarInterface, RegularStream, ExtendedLoggerInterface,
        ContentFormatInterface, ContentType, ConnType, ItemType, StreamType, StreamItemType, LoggingLevel,
        ARRAY_TYPES, Array, Name, Count, OptionalFields, Links, Context,
    )
    from base.constants.chars import CROP_SUFFIX
    from base.functions.arguments import update, get_str_from_args_kwargs, get_cropped_text
    from base.functions.errors import get_type_err_msg
    from streams.stream_builder import StreamBuilder
    from content.struct.flat_struct import FlatStruct
    from connectors.abstract.leaf_connector import LeafConnector, DEFAULT_EXAMPLE_COUNT
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        ConnectorInterface, StructInterface, ColumnarInterface, RegularStream, ExtendedLoggerInterface,
        ContentFormatInterface, ContentType, ConnType, ItemType, StreamType, StreamItemType, LoggingLevel,
        ARRAY_TYPES, Array, Name, Count, OptionalFields, Links, Context,
    )
    from ...base.constants.chars import CROP_SUFFIX
    from ...base.functions.arguments import update, get_str_from_args_kwargs, get_cropped_text
    from ...base.functions.errors import get_type_err_msg
    from ...streams.stream_builder import StreamBuilder
    from ...content.struct.flat_struct import FlatStruct
    from ..abstract.leaf_connector import LeafConnector, DEFAULT_EXAMPLE_COUNT

Native = LeafConnector
Stream = Union[RegularStream, ColumnarInterface]
GeneralizedStruct = Union[StructInterface, list, tuple, None]

META_MEMBER_MAPPING = dict(_source='database')
MAX_ITEMS_IN_MEMORY = 10000
EXAMPLE_STR_LEN = 12


class Table(LeafConnector):
    def __init__(
            self,
            name: Name,
            database: ConnectorInterface,
            content_format: Optional[ContentFormatInterface] = None,
            struct: Optional[StructInterface] = None,
            caption: Optional[str] = None,
            streams: Links = None,
            context: Context = None,
            reconnect: bool = False,
            expected_count: Count = None,
            verbose: Optional[bool] = None,
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
                raise TypeError(f'{content_class.__name__}: {e}')
        else:
            return content_type

    def _get_detected_struct(
            self,
            set_struct: bool = False,
            use_declared_types: Optional[bool] = None,
            skip_missing: bool = False,
            verbose: Optional[bool] = None,
    ) -> GeneralizedStruct:
        struct = self.get_struct_from_database(set_struct=set_struct)
        if struct:
            if verbose is None:
                verbose = self.is_verbose()
            if not isinstance(struct, StructInterface) and verbose:
                message = get_type_err_msg(struct, arg='struct', expected=FlatStruct, caller=self._get_detected_struct)
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
        return self.get_expected_count() is not None and self.get_initial_struct()

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
            if count is not None:
                must_recount = False
            elif allow_slow_mode:
                must_recount = self.is_existing()
            else:
                must_recount = False
        if must_recount:
            count = self.get_actual_lines_count(allow_slow_mode=allow_slow_mode)
            self.set_count(count)
        return count

    def get_actual_lines_count(
            self,
            allow_slow_mode: bool = False,
            verbose: Optional[bool] = None,
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
        elif struct is None:
            struct = self._get_struct_from_source()
        else:
            msg = get_type_err_msg(expected=(StructInterface, tuple), got=struct, arg='struct', caller=self.set_struct)
            raise TypeError(msg)
        return super().set_struct(struct, inplace=inplace)

    def get_struct_from_database(
            self,
            types: Links = None,
            set_struct: bool = False,
            skip_missing: bool = False,
            verbose: Optional[bool] = None,
    ) -> StructInterface:
        struct = FlatStruct(self.describe_table(verbose=verbose))
        if struct.is_empty() and not skip_missing:
            raise ValueError(f'Can not get struct for non-existing table {self}')
        if types is not None:
            struct.set_types(types, inplace=True)
        if set_struct:
            self.set_struct(struct, inplace=True)
        return struct

    def _get_struct_from_source(
            self,
            types: Optional[dict] = None,
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
            verbose: Optional[bool] = None,
            message: Optional[str] = None,
            step: Count = None,
    ) -> Iterator[str]:
        if skip_missing:
            if not self.is_existing():
                yield from []
        yield from self.execute_select(fields='*', count=count, verbose=verbose)
        self.close()

    def get_rows(self, verbose: Optional[bool] = None, step: Count = None) -> Iterable:
        database = self.get_database()
        return database.select_all(self.get_name(), verbose=verbose)

    def get_data(self, verbose: Optional[bool] = None) -> Iterable:
        return self.get_rows(verbose=verbose)

    def get_items(self, verbose: Optional[bool] = None, step: Count = None) -> Iterable:
        return self.get_rows(verbose=verbose, step=step)

    def get_items_of_type(
            self,
            item_type: Optional[ItemType],
            verbose: Optional[bool] = None,
            message: Optional[str] = None,
            step: Count = None,
    ) -> Iterable:
        if item_type in (ItemType.Auto, None):
            item_type = self.get_item_type()
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
            raise ValueError(f'Table.get_items_of_type(): cannot convert Rows to {item_type}')
        if step:
            logger = self.get_logger()
            if isinstance(logger, ExtendedLoggerInterface):
                count = self._get_fast_count()
                if message is None:
                    message = 'Downloading {count} lines from {name}'
                if '{}' in message:
                    message = message.format(count, self.get_name())
                if '{' in message:
                    message = message.format(count=count, name=self.get_name())
                items = logger.progress(items, name=message, count=count, step=step, context=self.get_context())
        return items

    def from_stream(self, stream: RegularStream, **kwargs) -> Native:
        if isinstance(stream, RegularStream) or hasattr(stream, 'get_items'):
            self.upload(data=stream, **kwargs)
        else:
            msg = get_type_err_msg(expected=RegularStream, got=stream, arg='stream', caller=self.from_stream)
            raise TypeError(msg)
        return self

    def is_existing(self, verbose: Optional[bool] = None) -> bool:
        database = self.get_database()
        return database.exists_table(self.get_path(), verbose=verbose)

    def describe_table(self, verbose: Optional[bool] = None) -> Iterable:
        database = self.get_database()
        return database.describe_table(self.get_path(), verbose=verbose)

    def create(self, drop_if_exists: bool, verbose: Optional[bool] = None):
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
            verbose: Optional[bool] = None,
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
            data: Optional[Iterable] = None,
            name: Optional[Name] = None,
            stream_type: ItemType = ItemType.Auto,
            ex: OptionalFields = None,
            step: Count = None,
            **kwargs
    ) -> Stream:  # SqlStream
        if stream_type in (ItemType.Auto, None):
            stream_type = StreamType.SqlStream
        if stream_type == StreamType.SqlStream:
            assert data is None, 'Table.to_stream(): For getting SqlStream data-argument must not be provided'
            if name is None:
                name = self._get_generated_stream_name()
            stream_example = StreamBuilder.empty(stream_type=stream_type)
            meta = self.get_compatible_meta(stream_example, name=name, ex=ex, **kwargs)
            meta['source'] = self
            return StreamBuilder.stream(data, **meta)
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
            verbose: Optional[bool] = None,
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
            stream_type: ItemType = ItemType.Auto,
            verbose: Optional[bool] = None,
    ) -> Stream:
        if stream_type in (ItemType.Auto, None):
            stream_type = ItemType.Record
        stream_rows = self.execute_select(fields=fields, filters=filters, sort=sort, count=count, verbose=verbose)
        if stream_type in (StreamType.RowStream, ItemType.Row):
            stream_data = stream_rows
        elif stream_type in (StreamType.RecordStream, ItemType.Record):
            columns = self.get_columns()
            stream_data = map(lambda r: dict(zip(columns, r)), stream_rows)
        else:
            raise NotImplementedError
        if count is not None:
            if count < MAX_ITEMS_IN_MEMORY:
                stream_data = list(stream_data)
                count = len(stream_data)
        return StreamBuilder.stream(stream_data, count=count, source=self, context=self.get_context())

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
            example_row_count: Optional[int] = DEFAULT_EXAMPLE_COUNT,
            example_str_len: Optional[int] = EXAMPLE_STR_LEN,
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
                message = f'Example with filters: {str_filters}'
            else:
                message = 'Example without any filters:'
        else:
            tag = '[EXAMPLE_NOT_FOUND]'
            message = f'{tag} Example with this filters not found: {str_filters}'
            stream_example = None
            item_example = self.get_one_item()
        if item_example:
            if example_str_len is not None:
                for k, v in item_example.items():
                    item_example[k] = get_cropped_text(v, max_len=example_str_len)
        else:
            item_example = dict()
            stream_example = None
            tag = '[EMPTY_DATA]'
            message = f'{tag} There are no valid data in {repr(self)}'
        return item_example, stream_example, message


ConnType.add_classes(Table)
