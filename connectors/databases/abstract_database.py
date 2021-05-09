from abc import ABC, abstractmethod
from typing import Union, Optional, Iterable, NoReturn

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from loggers import logger_classes as log
    from connectors import connector_classes as ct
    from streams import stream_classes as sm
    from schema import schema_classes as sh
    from fields.schema_interface import SchemaInterface
    from base.interfaces.data_interface import SimpleDataInterface
    from base.interfaces.context_interface import ContextInterface
    from connectors.abstract.connector_interface import ConnectorInterface
    from streams.interfaces.regular_stream_interface import RegularStreamInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...loggers import logger_classes as log
    from .. import connector_classes as ct
    from ...streams import stream_classes as sm
    from ...schema import schema_classes as sh
    from ...fields.schema_interface import SchemaInterface
    from ...base.interfaces.data_interface import SimpleDataInterface
    from ...base.interfaces.context_interface import ContextInterface
    from ..abstract.connector_interface import ConnectorInterface
    from ...streams.interfaces.regular_stream_interface import RegularStreamInterface

Stream = RegularStreamInterface
Name = str
Schema = Optional[SchemaInterface]
Table = ConnectorInterface
File = ct.AbstractFile
Data = Union[Stream, File, Table, str, Iterable]
OptBool = Union[bool, arg.DefaultArgument]

AUTO = arg.DEFAULT
TEST_QUERY = 'SELECT now()'
DEFAULT_GROUP = 'PUBLIC'
DEFAULT_STEP = 1000
DEFAULT_ERRORS_THRESHOLD = 0.05


class AbstractDatabase(ct.AbstractStorage, ABC):
    def __init__(
            self, name: str,
            host: str, port: int, db: str, user: str, password: str,
            verbose: OptBool = arg.DEFAULT, context: Union[ContextInterface, arg.DefaultArgument] = arg.DEFAULT,
            **kwargs
    ):
        super().__init__(
            name=name,
            context=arg.undefault(context, ct.get_context()),
            verbose=verbose,
        )
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = password
        self.conn_kwargs = kwargs
        self.connection = None
        self.LoggingLevel = log.LoggingLevel

    @staticmethod
    def get_default_child_class():
        return ct.Table

    def get_tables(self) -> dict:
        return self.get_children()

    def table(self, table: Union[Table, Name], schema: Schema = None, **kwargs) -> Table:
        table_name, schema = self._get_table_name_and_schema(table, schema, check_schema=False)
        table = self.get_tables().get(table_name)
        if table:
            assert not kwargs, 'table connection {} is already registered'.format(table_name)
        else:
            assert schema is not None, 'for create table schema must be defined'
            table = ct.Table(table_name, schema=schema, database=self, **kwargs)
            self.get_tables()[table_name] = table
        return table

    def close(self) -> int:
        if hasattr(self, 'disconnect'):
            return self.disconnect()

    def get_links(self) -> Iterable:
        for item in self.get_items():
            yield from item.get_links()

    @classmethod
    def need_connection(cls) -> bool:
        return hasattr(cls, 'connection')

    @classmethod
    def get_dialect_name(cls) -> str:
        return ct.get_dialect_type(cls.__name__)

    @abstractmethod
    def exists_table(self, name: Name, verbose: OptBool = arg.DEFAULT) -> bool:
        pass

    @abstractmethod
    def describe_table(self, name: Name, verbose: OptBool = arg.DEFAULT) -> bool:
        pass

    @abstractmethod
    def execute(
            self, query: str,
            get_data: OptBool = arg.DEFAULT, commit: OptBool = arg.DEFAULT, verbose: OptBool = arg.DEFAULT,
    ) -> Optional[Iterable]:
        pass

    def execute_query_from_file(
            self, file: File,
            get_data: OptBool = arg.DEFAULT, commit: OptBool = arg.DEFAULT, verbose: OptBool = arg.DEFAULT,
    ) -> Optional[Iterable]:
        assert isinstance(file, ct.TextFile), 'file must be TextFile, got {}'.format(file)
        query = '\n'.join(file.get_items())
        return self.execute(query, get_data=get_data, commit=commit, verbose=verbose)

    def execute_if_exists(
            self, query: str, table: Union[Table, Name],
            message_if_yes: Optional[str] = None, message_if_no: Optional[str] = None,
            stop_if_no: bool = False, verbose: OptBool = arg.DEFAULT,
    ) -> Optional[Iterable]:
        verbose = arg.undefault(verbose, message_if_yes or message_if_no)
        table_name = self._get_table_name(table)
        table_exists = self.exists_table(table_name, verbose=verbose)
        if table_exists:
            if '{}' in query:
                query = query.format(table_name)
            result = self.execute(query, verbose=verbose)
            if message_if_yes:
                if '{}' in message_if_yes:
                    message_if_yes = message_if_yes.format(table_name)
                self.log(message_if_yes, verbose=verbose)
            return result
        else:
            if message_if_no and '{}' in message_if_no:
                message_if_no = message_if_no.format(table_name)
            if stop_if_no:
                raise ValueError(message_if_no)
            else:
                if message_if_no:
                    self.log(message_if_no, verbose=verbose)

    def create_table(
            self, table: Union[Table, Name], schema: Schema,
            drop_if_exists: bool = False, verbose: OptBool = arg.DEFAULT,
    ) -> NoReturn:
        verbose = arg.undefault(verbose, self.verbose)
        table_name, schema_str = self._get_table_name_and_schema_str(table, schema, check_schema=True)
        if drop_if_exists:
            self.drop_table(table_name, verbose=verbose)
        message = 'Creating table:'
        query = 'CREATE TABLE {name} ({schema});'.format(
            name=table_name,
            schema=schema_str,
        )
        self.execute(
            query, get_data=False, commit=True,
            verbose=message if verbose is True else verbose,
        )
        self.post_create_action(table_name, verbose=verbose)
        self.log('Table {name} is created.'.format(name=table_name), verbose=verbose)

    def post_create_action(self, name: Name, **kwargs):
        pass

    def drop_table(self, table: Union[Table, Name], if_exists: bool = True, verbose: OptBool = arg.DEFAULT) -> NoReturn:
        self.execute_if_exists(
            query='DROP TABLE IF EXISTS {};',
            table=table,
            message_if_yes='Table {} has been dropped',
            message_if_no='Table {} did not exists before, nothing dropped.',
            stop_if_no=not if_exists,
            verbose=verbose,
        )

    def copy_table(
            self, old: Union[Table, Name], new: Union[Table, Name],
            if_exists: bool = False, verbose: OptBool = arg.DEFAULT,
    ) -> NoReturn:
        name_old = self._get_table_name(old)
        name_new = self._get_table_name(new)
        cat_old, name_old = name_old.split('.')
        cat_new, name_new = name_new.split('.') if '.' in new else (cat_old, new)
        assert cat_new == cat_old, 'Can copy within same scheme (folder) only (got {} != {})'.format(cat_old, cat_new)
        new = name_new
        self.execute_if_exists(
            query='CREATE TABLE {new} AS TABLE {old};'.format(new=new, old=old),
            table=old,
            message_if_yes='Table {old} is copied to {new}'.format(old=old, new=new),
            message_if_no='Can not copy table {}: not exists',
            stop_if_no=not if_exists,
            verbose=verbose,
        )

    def rename_table(
            self, old: Union[Table, Name], new: Union[Table, Name],
            if_exists: bool = False, verbose: OptBool = arg.DEFAULT,
    ) -> NoReturn:
        name_old = self._get_table_name(old)
        name_new = self._get_table_name(new)
        cat_old, name_old = old.split('.')
        cat_new, name_new = new.split('.') if '.' in new else (cat_old, new)
        assert cat_new == cat_old, 'Can copy within same scheme (folder) only (got {} and {})'.format(cat_new, cat_old)
        new = name_new
        self.execute_if_exists(
            query='ALTER TABLE {old} RENAME TO {new};'.format(old=old, new=new),
            table=old,
            message_if_yes='Table {old} is renamed to {new}'.format(old=old, new=new),
            message_if_no='Can not rename table {}: not exists.',
            stop_if_no=not if_exists,
            verbose=verbose,
        )

    def select(
            self, table: Union[Table, Name],
            fields: Union[Iterable, str], filters: Union[Optional[Iterable], str] = None,
            verbose: OptBool = arg.DEFAULT,
    ) -> Iterable:
        fields_str = fields if isinstance(fields, str) else ', '.join(fields)
        filters_str = filters if isinstance(filters, str) else ' AND '.join(filters) if filters is not None else ''
        table_name = self._get_table_name(table)
        if filters:
            query = 'SELECT {fields} FROM {table} WHERE {filters};'.format(
                table=table_name,
                fields=fields_str,
                filters=filters_str,
            )
        else:
            query = 'SELECT {fields} FROM {table};'.format(
                table=table_name,
                fields=fields_str,
            )
        return self.execute(query, get_data=True, commit=False, verbose=verbose)

    def select_count(self, table: Union[Table, Name], verbose: OptBool = arg.DEFAULT) -> int:
        return self.select(table, fields='COUNT(*)', verbose=verbose)[0][0]

    def select_all(self, table: Union[Table, Name], verbose=arg.DEFAULT):
        return self.select(table, fields='*', verbose=verbose)

    @staticmethod
    def _get_schema_stream_from_data(data: Data, schema: Schema = None, **file_kwargs) -> Stream:
        if sm.is_stream(data):
            stream = data
        elif ct.is_file(data):
            stream = data.to_schema_stream()
            if schema:
                assert stream.get_columns() == schema.get_columns()
        elif isinstance(data, str):
            stream = sm.RowStream.from_column_file(filename=data, **file_kwargs)
        else:
            stream = sm.AnyStream(data)
        return stream

    @abstractmethod
    def insert_rows(
            self,
            name: str, rows: Iterable, columns: Iterable,
            step: int = DEFAULT_STEP, skip_errors: bool = False,
            expected_count: Union[Optional[int], arg.DefaultArgument] = arg.DefaultArgument, return_count: bool = True,
            verbose: OptBool = arg.DEFAULT,
    ) -> Optional[int]:
        pass

    def insert_struct_stream(
            self, table: Union[Table, Name], stream: Stream,
            skip_errors: bool = False, step: int = DEFAULT_STEP, verbose: OptBool = arg.DEFAULT,
    ) -> Optional[int]:
        columns = stream.get_columns()
        assert columns, 'columns in StructStream must be defined (got {})'.format(stream)
        if hasattr(table, 'get_columns'):
            table_cols = table.get_columns()
            assert columns == table_cols, '{} != {}'.format(columns, table_cols)
        table_name = self._get_table_name(table)
        expected_count = stream.get_count()
        final_count = stream.get_calc(
            lambda a: self.insert_rows(
                table_name, rows=a, columns=columns,
                step=step, expected_count=expected_count,
                skip_errors=skip_errors, return_count=True,
                verbose=verbose,
            ),
        )
        return final_count

    def insert_data(
            self,
            table: Union[Table, Name], data: Data, schema: Schema = None,
            encoding: Optional[str] = None, skip_errors: bool = False,
            skip_lines: Optional[int] = 0, skip_first_line: bool = False,
            step: Union[int, arg.DefaultArgument] = DEFAULT_STEP, verbose: OptBool = arg.DEFAULT,
    ) -> tuple:
        if not arg.is_defined(skip_lines):
            skip_lines = 0
        is_schema_description = isinstance(schema, sh.SchemaDescription) or hasattr(schema, 'get_schema_str')
        if not is_schema_description:
            message = 'Schema as {} is deprecated, use sh.SchemaDescription instead'.format(type(schema))
            self.log(msg=message, level=log.LoggingLevel.Warning)
            schema = sh.SchemaDescription(schema or [])
        input_stream = self._get_schema_stream_from_data(
            data, schema=schema,
            encoding=encoding, skip_first_line=skip_first_line, verbose=verbose,
        )
        if skip_lines:
            input_stream = input_stream.skip(skip_lines)
        if input_stream.get_stream_type() != sm.StreamType.SchemaStream:
            input_stream = input_stream.schematize(
                schema,
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
            table: Union[Table, Name], schema: Schema, data: Data,
            encoding: Optional[str] = None, step: int = DEFAULT_STEP,
            skip_lines: int = 0, skip_first_line: bool = False, max_error_rate: float = 0.0,
            verbose: OptBool = arg.DEFAULT,
    ) -> NoReturn:
        verbose = arg.undefault(verbose, self.verbose)
        table_name, schema = self._get_table_name_and_schema(table, schema)
        if not skip_lines:
            self.create_table(table_name, schema=schema, drop_if_exists=True, verbose=verbose)
        skip_errors = (max_error_rate is None) or (max_error_rate > DEFAULT_ERRORS_THRESHOLD)
        initial_count, write_count = self.insert_data(
            table, schema=schema, data=data,
            encoding=encoding, skip_first_line=skip_first_line,
            step=step, skip_lines=skip_lines, skip_errors=skip_errors,
            verbose=verbose,
        )
        write_count += (skip_lines if isinstance(skip_lines, int) else 0)  # can be None or arg.default
        result_count = self.select_count(table)
        if write_count:
            error_rate = (write_count - result_count) / write_count
            message = 'Check counts: {} initial, {} uploaded, {} written, {} error_rate'
        else:
            error_rate = 1.0
            message = 'ERR: Data {} and/or Table {} is empty.'.format(data, table)
        self.log(message.format(initial_count, write_count, result_count, error_rate), verbose=verbose)
        if max_error_rate is not None:
            message = 'Too many errors or skipped lines ({} > {})'.format(error_rate, max_error_rate)
            assert error_rate < max_error_rate, message

    def safe_upload_table(
            self,
            table: Union[Table, Name], schema: Schema, data: Data,
            encoding: Optional[str] = None,
            step: int = DEFAULT_STEP,
            skip_lines: int = 0, skip_first_line: bool = False, max_error_rate: float = 0.0,
            verbose: OptBool = arg.DEFAULT,
    ):
        target_name, schema = self._get_table_name_and_schema(table, schema)
        tmp_name = '{}_tmp_upload'.format(target_name)
        bak_name = '{}_bak'.format(target_name)
        self.force_upload_table(
            table=tmp_name, schema=schema, data=data, encoding=encoding, skip_first_line=skip_first_line,
            step=step, skip_lines=skip_lines, max_error_rate=max_error_rate,
            verbose=verbose,
        )
        self.drop_table(bak_name, if_exists=True, verbose=verbose)
        self.rename_table(table, bak_name, if_exists=True, verbose=verbose)
        self.rename_table(tmp_name, target_name, if_exists=True, verbose=verbose)

    @staticmethod
    def _get_table_name(table: Union[Table, Name]) -> str:
        if hasattr(table, 'get_name'):
            return table.get_name()
        else:
            return str(table)

    @staticmethod
    def _get_table_name_and_schema(
            table: Union[Table, Name], expected_schema: Schema = None,
            check_schema: bool = True,
    ) -> tuple:
        if isinstance(table, Name):
            table_name = table
            table_schema = expected_schema
        else:
            table_name = table.get_name()
            if hasattr(table, 'get_schema'):
                table_schema = table.get_schema()
                if expected_schema and check_schema:
                    assert table_schema == expected_schema
            else:
                table_schema = expected_schema
        if check_schema:
            assert table_schema, 'schema must be defined'
        return table_name, table_schema

    def _get_table_name_and_schema_str(
            self,
            table: Union[Table, Name], expected_schema: Schema = None,
            check_schema: bool = True,
    ):
        table_name, schema = self._get_table_name_and_schema(table, expected_schema, check_schema=check_schema)
        if isinstance(schema, str):
            schema_str = schema
            message = 'String Schemas is deprecated. Use schema.SchemaDescription instead.'
            self.log(msg=message, level=log.LoggingLevel.Warning)
        elif isinstance(schema, (list, tuple)):
            schema_str = ', '.join(['{} {}'.format(c[0], c[1]) for c in schema])
            message = 'Tuple Schemas is deprecated. Use schema.SchemaDescription instead.'
            self.log(msg=message, level=log.LoggingLevel.Warning)
        elif hasattr(schema, 'get_schema_str'):
            schema_str = schema.get_schema_str(dialect=self.get_dialect_name())
        else:
            raise TypeError('schema must be an instance of SchemaDescription or list[tuple]')
        return table_name, schema_str
