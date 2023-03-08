from abc import ABC, abstractmethod
from typing import Optional, Iterable, Tuple, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        StreamInterface, ColumnarInterface, ColumnarStream, StructStream, StructInterface, SimpleDataInterface,
        LeafConnectorInterface, ConnType, StreamType, DialectType, LoggingLevel,
        Context, Count, Name, FieldName, OptionalFields, Connector,
    )
    from base.constants.chars import (
        EMPTY, PARAGRAPH_CHAR, TAB_CHAR,
        ITEMS_DELIMITER, DOT, ALL, SEMICOLON, PY_PLACEHOLDER,
    )
    from loggers.fallback_logger import FallbackLogger
    from functions.primary import text as tx
    from content.struct.flat_struct import FlatStruct
    from streams.stream_builder import StreamBuilder
    from connectors.abstract.abstract_storage import AbstractStorage
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        StreamInterface, ColumnarInterface, ColumnarStream, StructStream, StructInterface, SimpleDataInterface,
        LeafConnectorInterface, ConnType, StreamType, DialectType, LoggingLevel,
        Context, Count, Name, FieldName, OptionalFields, Connector,
    )
    from ...base.constants.chars import (
        EMPTY, PARAGRAPH_CHAR, TAB_CHAR,
        ITEMS_DELIMITER, DOT, ALL, SEMICOLON, PY_PLACEHOLDER,
    )
    from ...loggers.fallback_logger import FallbackLogger
    from ...functions.primary import text as tx
    from ...content.struct.flat_struct import FlatStruct
    from ...streams.stream_builder import StreamBuilder
    from ..abstract.abstract_storage import AbstractStorage

Native = AbstractStorage
Struct = Optional[StructInterface]
Table = Connector
File = LeafConnectorInterface
Data = Union[ColumnarStream, File, Table, Iterable]

TEST_QUERY = 'SELECT now()'
DEFAULT_GROUP = 'PUBLIC'
DEFAULT_STEP = 1000
DEFAULT_ERRORS_THRESHOLD = 0.05
COVERT_PROPS = ('password', )


class AbstractDatabase(AbstractStorage, ABC):
    def __init__(
            self,
            name: Name,
            host: str,
            port: int,
            db: str,
            user: Optional[str] = None,
            password: Optional[str] = None,
            context: Context = None,
            verbose: Optional[bool] = None,
            **kwargs
    ):
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = password
        self.conn_kwargs = kwargs
        self.connection = None
        super().__init__(name=name, context=context, verbose=verbose)

    @staticmethod
    def get_default_child_type() -> ConnType:
        return ConnType.Table

    def get_tables(self) -> dict:
        return self.get_children()

    def table(self, table: Union[Table, Name], struct: Struct = None, **kwargs) -> Table:
        table_name, struct = self._get_table_name_and_struct(table, struct, check_struct=False)
        table = self.get_tables().get(table_name)
        if table:
            assert not kwargs, f'table connection {table_name} is already registered'
        else:
            table_class = self.get_default_child_obj_class()
            table = table_class(table_name, struct=struct, database=self, **kwargs)
            self.get_tables()[table_name] = table
        return table

    def close(self) -> int:
        if hasattr(self, 'disconnect'):
            return self.disconnect()

    def is_accessible(self, verbose: bool = False) -> bool:
        try:
            self.execute(query=TEST_QUERY, verbose=verbose)
            return True
        except:  # OperationalError
            return False

    def get_links(self) -> Iterable:
        for item in self.get_items():
            yield from item.get_links()

    @classmethod
    def need_connection(cls) -> bool:
        return hasattr(cls, 'connection')

    @classmethod
    def get_dialect_type(cls) -> DialectType:
        dialect_type = DialectType.find_instance(cls.__name__)
        assert isinstance(dialect_type, DialectType)
        return dialect_type

    @classmethod
    def get_dialect_name(cls) -> str:
        return cls.get_dialect_type().get_value()

    @abstractmethod
    def exists_table(self, name: Name, verbose: Optional[bool] = None) -> bool:
        pass

    @abstractmethod
    def describe_table(self, name: Name, verbose: Optional[bool] = None) -> Iterable:
        pass

    def _get_execution_message(self, query: str, verbose: Union[str, bool]) -> str:
        query_repr = repr(self._get_compact_query_view(query))
        db_name = self.get_name()
        template = verbose if isinstance(verbose, str) else '{db}.execute({query})'
        if '{query}' in template:
            if '{db}' in template:
                return template.format(db=db_name, query=query_repr)
            else:
                return template.format(query=query_repr)
        elif PY_PLACEHOLDER in template:
            return template.format(query_repr)
        else:
            return template

    @abstractmethod
    def execute(
            self,
            query: str,
            get_data: Optional[bool] = None,
            commit: Optional[bool] = None,
            verbose: Optional[bool] = None,
    ) -> Optional[Iterable]:
        pass

    def execute_query_from_file(
            self,
            file: File,
            get_data: Optional[bool] = None,
            commit: Optional[bool] = None,
            verbose: Optional[bool] = None,
    ) -> Optional[Iterable]:
        assert isinstance(file, File) or hasattr(file, 'get_items'), f'file must be LocalFile, got {file}'
        query = PARAGRAPH_CHAR.join(file.get_items())
        return self.execute(query, get_data=get_data, commit=commit, verbose=verbose)

    def execute_if_exists(
            self,
            query: str,
            table: Union[Table, Name],
            message_if_yes: Optional[str] = None,
            message_if_no: Optional[str] = None,
            stop_if_no: bool = False,
            verbose: Optional[bool] = None,
    ) -> Optional[Iterable]:
        if verbose is not None:
            verbose = bool(message_if_yes or message_if_no)
        table_name = self._get_table_name(table)
        table_exists = self.exists_table(table_name, verbose=verbose)
        if table_exists:
            if PY_PLACEHOLDER in query:
                query = query.format(table_name)
            result = self.execute(query, verbose=verbose)
            if message_if_yes:
                if PY_PLACEHOLDER in message_if_yes:
                    message_if_yes = message_if_yes.format(table_name)
                self.log(message_if_yes, verbose=verbose)
            return result
        else:
            if message_if_no and PY_PLACEHOLDER in message_if_no:
                message_if_no = message_if_no.format(table_name)
            if stop_if_no:
                raise ValueError(message_if_no)
            else:
                if message_if_no:
                    self.log(message_if_no, verbose=verbose)

    def create_table(
            self,
            table: Union[Table, Name],
            struct: Struct,
            drop_if_exists: bool = False,
            verbose: Optional[bool] = None,
    ) -> Table:
        if verbose is None:
            verbose = self.verbose
        table_name, struct_str = self._get_table_name_and_struct_str(table, struct, check_struct=True)
        if drop_if_exists:
            self.drop_table(table_name, verbose=verbose)
        message = 'Creating table:'
        query = 'CREATE TABLE {name} ({struct});'.format(name=table_name, struct=struct_str)
        self.execute(
            query, get_data=False, commit=True,
            verbose=message if verbose is True else verbose,
        )
        self.post_create_action(table_name, verbose=verbose)
        self.log('Table {name} is created.'.format(name=table_name), verbose=verbose)
        if struct:
            return self.table(table, struct=struct)
        else:
            return self.table(table)

    def post_create_action(self, name: Name, **kwargs) -> None:
        pass

    def drop_table(self, table: Union[Table, Name], if_exists: bool = True, verbose: Optional[bool] = None) -> Native:
        self.execute_if_exists(
            query='DROP TABLE IF EXISTS {};',
            table=table,
            message_if_yes='Table {} has been dropped',
            message_if_no='Table {} did not exists before, nothing dropped.',
            stop_if_no=not if_exists,
            verbose=verbose,
        )
        return self

    def copy_table(
            self,
            old: Union[Table, Name],
            new: Union[Table, Name],
            if_exists: bool = False,
            verbose: Optional[bool] = None,
    ) -> Table:
        name_old = self._get_table_name(old)
        name_new = self._get_table_name(new)
        cat_old, name_old = name_old.split(DOT)
        cat_new, name_new = name_new.split(DOT) if DOT in new else (cat_old, new)
        assert cat_new == cat_old, f'Can copy within same scheme (folder) only (got {cat_old} != {cat_new})'
        new = name_new
        self.execute_if_exists(
            query=f'CREATE TABLE {new} AS TABLE {old};',
            table=old,
            message_if_yes=f'Table {old} is copied to {new}',
            message_if_no='Can not copy table {}: not exists',
            stop_if_no=not if_exists,
            verbose=verbose,
        )
        return self.table(new)

    def rename_table(
            self,
            old: Union[Table, Name],
            new: Union[Table, Name],
            if_exists: bool = False,
            verbose: Optional[bool] = None,
    ) -> Table:
        name_old = self._get_table_name(old)
        name_new = self._get_table_name(new)
        cat_old, table_old = name_old.split(DOT)
        cat_new, table_new = name_new.split(DOT) if DOT in name_new else (cat_old, name_new)
        assert cat_new == cat_old, f'Can copy within same scheme (folder) only (got {cat_new} and {cat_old})'
        table_connector_old = self.get_child(name_old)
        if isinstance(table_connector_old, LeafConnectorInterface) or hasattr(table_connector_old, 'get_struct'):
            struct = table_connector_old.get_struct()
        else:
            struct = None
        self.execute_if_exists(
            query='ALTER TABLE {old} RENAME TO {new};'.format(old=name_old, new=table_new),
            table=old,
            message_if_yes='Table {old} is renamed to {new}'.format(old=old, new=new),
            message_if_no='Can not rename table {}: not exists.',
            stop_if_no=not if_exists,
            verbose=verbose,
        )
        return self.table(name_new, struct=struct)

    def execute_select(
            self,
            table: Union[Table, Name],
            fields: OptionalFields,
            filters: OptionalFields = None,
            sort: OptionalFields = None,
            count: Count = None,
            verbose: Optional[bool] = None,
    ) -> Iterable:
        if isinstance(fields, str):
            fields_str = fields
        elif isinstance(fields, Iterable):
            fields_str = ITEMS_DELIMITER.join(fields)
        elif not fields:
            fields_str = ALL
        else:
            expected = 'str, Iterable or None'
            msg = f'Expected {expected}, got {fields}'
            raise TypeError(msg)
        filters_str = filters if isinstance(filters, str) else ' AND '.join(filters) if filters is not None else EMPTY
        sort_str = sort if isinstance(sort, str) else ' AND '.join(sort) if sort is not None else EMPTY
        table_name = self._get_table_name(table)
        query = 'SELECT {fields} FROM {table}'.format(table=table_name, fields=fields_str)
        if filters:
            query += ' WHERE {filters}'.format(filters=filters_str)
        if sort:
            query += ' ORDER BY {sort}'.format(sort=sort_str)
        if count:
            query += ' LIMIT {count}'.format(count=count)
        query += SEMICOLON
        return self.execute(query, get_data=True, commit=False, verbose=verbose)

    def select_count(self, table: Union[Table, Name], verbose: Optional[bool] = None) -> int:
        counted_field = ALL
        selected_fields = f'COUNT({counted_field})'
        response = self.execute_select(table, fields=selected_fields, verbose=verbose)
        count = list(response)[0][0]
        return count

    def select_all(self, table: Union[Table, Name], verbose: Optional[bool] = None) -> Iterable:
        return self.execute_select(table, fields=ALL, verbose=verbose)

    @abstractmethod
    def insert_rows(
            self,
            name: str,
            rows: Iterable,
            columns: Iterable,
            step: int = DEFAULT_STEP,
            skip_errors: bool = False,
            expected_count: Count = None,
            return_count: bool = True,
            verbose: Optional[bool] = None,
    ) -> Count:
        pass

    def insert_struct_stream(
            self,
            table: Union[Table, Name],
            stream: StructStream,
            skip_errors: bool = False,
            step: int = DEFAULT_STEP,
            verbose: Optional[bool] = None,
    ) -> Count:
        if hasattr(stream, 'get_columns'):
            columns = stream.get_columns()
            assert columns, f'Columns in StructStream must be defined (got {stream})'
        else:
            expected = 'StructStream'
            msg = f'{self}.insert_struct_stream(): Expected {expected}, got {stream} as {type(stream)}'
            raise TypeError(msg)
        assert hasattr(stream, 'get_struct')  # isinstance(stream, StructStream)
        if hasattr(table, 'get_columns'):
            table_cols = table.get_columns()
            assert columns == table_cols, f'{columns} != {table_cols}'
        table_name = self._get_table_name(table)
        expected_count = stream.get_count()
        final_count = self.insert_rows(
            table_name, rows=stream.get_items(), columns=tuple(columns),
            step=step, expected_count=expected_count,
            skip_errors=skip_errors, return_count=True,
            verbose=verbose,
        )
        return final_count

    def insert_data(
            self,
            table: Union[Table, Name],
            data: Data, struct: Struct = None,
            encoding: Optional[str] = None,
            skip_errors: bool = False,
            skip_lines: Count = 0,
            skip_first_line: bool = False,
            step: Count = DEFAULT_STEP,
            verbose: Optional[bool] = None,
    ) -> tuple:
        if skip_lines is None:
            skip_lines = 0
        is_struct_description = isinstance(struct, StructInterface) or hasattr(struct, 'get_struct_str')
        if not is_struct_description:
            message = f'Struct as {type(struct)} is deprecated, use FlatStruct instead'
            self.log(msg=message, level=LoggingLevel.Warning)
            struct = FlatStruct(struct or [])
        input_stream = self._get_struct_stream_from_data(
            data, struct=struct,
            encoding=encoding, skip_first_line=skip_first_line, verbose=verbose,
        )
        if skip_lines:
            input_stream = input_stream.skip(skip_lines)
        if input_stream.get_struct() is None:
            input_stream = input_stream.structure(
                struct,
                skip_bad_rows=True,
                verbose=True,
            ).update_meta(
                count=input_stream.get_count(),
            )
        initial_count = input_stream.get_estimated_count() + skip_lines
        final_count = self.insert_struct_stream(
            table, input_stream,
            skip_errors=skip_errors, step=step,
            verbose=verbose,
        )
        return initial_count, final_count

    def force_upload_table(
            self,
            table: Union[Table, Name],
            struct: Struct, data: Data,
            encoding: Optional[str] = None,
            step: Count = DEFAULT_STEP,
            skip_lines: Count = 0,
            skip_first_line: bool = False,
            max_error_rate: float = 0.0,
            verbose: Optional[bool] = None,
    ) -> Table:
        if verbose is None:
            verbose = self.verbose
        table_name, struct = self._get_table_name_and_struct(table, struct)
        if not skip_lines:
            self.create_table(table_name, struct=struct, drop_if_exists=True, verbose=verbose)
        skip_errors = (max_error_rate is None) or (max_error_rate > DEFAULT_ERRORS_THRESHOLD)
        initial_count, write_count = self.insert_data(
            table, struct=struct, data=data,
            encoding=encoding, skip_first_line=skip_first_line,
            step=step, skip_lines=skip_lines, skip_errors=skip_errors,
            verbose=verbose,
        )
        if skip_lines is not None:
            write_count += skip_lines
        result_count = self.select_count(table)
        if write_count:
            error_rate = (write_count - result_count) / write_count
            template = 'Check counts: {} initial, {} uploaded, {} written, {} error_rate'
            message = template.format(initial_count, write_count, result_count, error_rate)
        else:
            error_rate = 1.0
            message = f'ERR: Data {data} and/or Table {table} is empty.'
        self.log(message, verbose=verbose)
        if max_error_rate is not None:
            message = f'Too many errors or skipped lines ({error_rate} > {max_error_rate})'
            assert error_rate < max_error_rate, message
        return self.table(table, struct=struct)

    def safe_upload_table(
            self,
            table: Union[Table, Name],
            struct: Struct, data: Data,
            encoding: Optional[str] = None,
            step: int = DEFAULT_STEP,
            skip_lines: int = 0,
            skip_first_line: bool = False,
            max_error_rate: float = 0.0,
            verbose: Optional[bool] = None,
    ) -> Table:
        target_name, struct = self._get_table_name_and_struct(table, struct)
        tmp_name = f'{target_name}_tmp_upload'
        bak_name = f'{target_name}_bak'
        self.force_upload_table(
            table=tmp_name, struct=struct, data=data, encoding=encoding, skip_first_line=skip_first_line,
            step=step, skip_lines=skip_lines, max_error_rate=max_error_rate,
            verbose=verbose,
        )
        self.drop_table(bak_name, if_exists=True, verbose=verbose)
        self.rename_table(table, bak_name, if_exists=True, verbose=verbose)
        self.rename_table(tmp_name, target_name, if_exists=True, verbose=verbose)
        return self.table(table)

    def get_credentials(self) -> tuple:
        return self.user, self.password

    def set_credentials(self, user: str, password: str, verbose: bool = True) -> Native:
        self.user = user
        self.password = password
        self.log(f'Credentials for {repr(self)} has been updated', verbose=verbose)
        return self

    def take_credentials_from_file(self, file: Union[File, Name], by_name: bool = False, delimiter=None) -> Native:
        if delimiter is None:
            if by_name:
                delimiter = TAB_CHAR
            else:
                delimiter = PARAGRAPH_CHAR
        if isinstance(file, str):
            context = self.get_context()
            if context:
                folder = context.get_job_folder()
            else:
                storage = ConnType.LocalStorage.get_class()
                folder = storage().folder(EMPTY)
            file = folder.file(name=file)
        parsed_file = self._parse_credentials_file(file, delimiter=delimiter)
        if by_name:
            database_name = self.get_name()
            credentials = tuple()
            for name, user, password in parsed_file:
                if name == database_name:
                    credentials = user, password
                    break
        else:
            credentials = list(parsed_file)[0]
        if credentials:
            self.set_credentials(*credentials)
        return self

    @classmethod
    def _parse_credentials_file(cls, file: File, delimiter: str = PARAGRAPH_CHAR) -> Iterable:
        if isinstance(file, File) or hasattr(file, 'to_line_stream'):
            has_columns = delimiter != PARAGRAPH_CHAR
            if has_columns:
                for item in file.to_line_stream().get_items():
                    yield item.split(delimiter)
            else:
                two_lines = file.to_line_stream().take(2)
                assert isinstance(two_lines, StreamInterface) or hasattr(two_lines, 'get_list')
                login, password = two_lines.get_list()[:2]
                yield login, password
        else:
            msg = f'{cls.__name__}._parse_credentials_file(): LocalFile expected, got {file}'
            raise TypeError(msg)

    @staticmethod
    def _get_compact_query_view(query: str) -> str:
        return tx.remove_extra_spaces(query)

    @staticmethod
    def _get_table_name(table: Union[Table, Name]) -> str:
        if hasattr(table, 'get_name'):
            return table.get_name()
        else:
            return str(table)

    @staticmethod
    def _get_schema_and_table_name(
            table: Union[Table, Name],
            default_schema: Optional[str] = None,
    ) -> Tuple[Optional[str], str]:
        if isinstance(table, LeafConnectorInterface) or hasattr(table, 'get_name'):
            name = table.get_name()
        elif isinstance(table, str):
            name = table
        else:
            expected_types = 'Table or Name'
            msg = f'Expected {expected_types}, got {table}'
            raise TypeError(msg)
        if DOT in name:
            schema_name, table_name = name.split(DOT)
        else:
            schema_name, table_name = default_schema, name
        return schema_name, table_name

    @classmethod
    def _get_table_name_and_struct(
            cls,
            table: Union[Table, Name],
            expected_struct: Struct = None,
            check_struct: bool = True,
    ) -> tuple:
        if isinstance(table, str):
            table_name = table
            table_struct = expected_struct
        elif cls._assert_is_appropriate_child(table) or hasattr(table, 'get_name'):
            table_name = table.get_name()
            if isinstance(table, LeafConnectorInterface) or hasattr(table, 'get_struct'):
                table_struct = table.get_struct()
                if expected_struct and check_struct:
                    assert table_struct == expected_struct, f'{table_struct} != {expected_struct}'
            else:
                table_struct = expected_struct
        else:
            expected_types = 'Table or Name'
            msg = f'Expected {expected_types}, got {table}'
            raise TypeError(msg)
        if check_struct:
            assert table_struct, 'struct must be defined'
        return table_name, table_struct

    @staticmethod
    def _get_struct_stream_from_data(data: Data, struct: Struct = None, **file_kwargs) -> StructStream:
        if isinstance(data, StreamInterface):
            stream = data
        elif isinstance(data, File) or hasattr(data, 'to_struct_stream'):
            stream = data.to_struct_stream()
            if struct:
                stream_cols = stream.get_columns()
                struct_cols = struct.get_columns()
                assert stream_cols == struct_cols, f'{stream_cols} != {struct_cols}'
        elif isinstance(data, str):  # deprecated
            logger = FallbackLogger()
            logger.warning('usage of filename as data-argument is deprecated, use file object instead')
            build_stream = StreamType.RowStream.get_class()
            stream = build_stream.from_column_file(filename=data, **file_kwargs)  # deprecated
        else:
            build_stream = StreamBuilder.get_default_stream_class()
            stream = build_stream(data)
        return stream

    def _get_table_name_and_struct_str(
            self,
            table: Union[Table, Name],
            expected_struct: Struct = None,
            check_struct: bool = True,
    ) -> tuple:
        table_name, struct = self._get_table_name_and_struct(table, expected_struct, check_struct=check_struct)
        if isinstance(struct, str):
            struct_str = struct
            message = 'String Struct is deprecated. Use items.FlatStruct instead.'
            self.log(msg=message, level=LoggingLevel.Warning)
        elif isinstance(struct, (list, tuple)):
            struct_str = ITEMS_DELIMITER.join([f'{c[0]} {c[1]}' for c in struct])
            message = 'Tuple-description of Struct is deprecated. Use items.FlatStruct instead.'
            self.log(msg=message, level=LoggingLevel.Warning)
        elif hasattr(struct, 'get_struct_str'):
            struct_str = struct.get_struct_str(dialect=self.get_dialect_type())
        else:
            expected_types = 'StructInterface or list[tuple]'
            msg = f'struct must be an instance of {expected_types}, got {struct}'
            raise TypeError(msg)
        return table_name, struct_str

    @staticmethod
    def _get_covert_props() -> tuple:
        return COVERT_PROPS
