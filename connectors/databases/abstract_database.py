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
    from streams.interfaces.regular_stream_interface import RegularStreamInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...loggers import logger_classes as log
    from .. import connector_classes as ct
    from ...streams import stream_classes as sm
    from ...schema import schema_classes as sh
    from ...fields.schema_interface import SchemaInterface
    from ...base.interfaces.data_interface import SimpleDataInterface
    from ...streams.interfaces.regular_stream_interface import RegularStreamInterface

Stream = RegularStreamInterface
Data = Union[Stream, sm.ConvertMixin, ct.AbstractFile, str, Iterable]

AUTO = arg.DEFAULT
TEST_QUERY = 'SELECT now()'
DEFAULT_GROUP = 'PUBLIC'
DEFAULT_STEP = 1000
DEFAULT_ERRORS_THRESHOLD = 0.05


class AbstractDatabase(ct.AbstractStorage, ABC):
    def __init__(self, name, host, port, db, user, password, verbose=arg.DEFAULT, context=arg.DEFAULT, **kwargs):
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

    def table(self, name, schema=None, **kwargs):
        table = self.get_tables().get(name)
        if table:
            assert not kwargs, 'table connection {} is already registered'.format(name)
        else:
            assert schema is not None, 'for create table schema must be defined'
            table = ct.Table(name, schema=schema, database=self, **kwargs)
            self.get_tables()[name] = table
        return table

    def close(self) -> int:
        if hasattr(self, 'disconnect'):
            return self.disconnect()

    def get_links(self):
        for item in self.get_items():
            yield from item.get_links()

    @classmethod
    def need_connection(cls) -> bool:
        return hasattr(cls, 'connection')

    def get_dialect_name(self):
        return ct.get_dialect_type(self.__class__.__name__)

    @abstractmethod
    def exists_table(self, name, verbose=arg.DEFAULT) -> bool:
        pass

    @abstractmethod
    def describe_table(self, name, verbose=arg.DEFAULT) -> bool:
        pass

    @abstractmethod
    def execute(self, query, get_data=arg.DEFAULT, commit=arg.DEFAULT, verbose=arg.DEFAULT):
        pass

    def execute_query_from_file(self, file, get_data=arg.DEFAULT, commit=arg.DEFAULT, verbose=arg.DEFAULT):
        assert isinstance(file, ct.TextFile)
        query = '\n'.join(file.get_items())
        return self.execute(query, get_data=get_data, commit=commit, verbose=verbose)

    def execute_if_exists(
            self, query, table,
            message_if_yes=None, message_if_no=None, stop_if_no=False, verbose=arg.DEFAULT,
    ):
        verbose = arg.undefault(verbose, message_if_yes or message_if_no)
        table_exists = self.exists_table(table, verbose=verbose)
        if table_exists:
            if '{}' in query:
                query = query.format(table)
            result = self.execute(query, verbose=verbose)
            if message_if_yes:
                if '{}' in message_if_yes:
                    message_if_yes = message_if_yes.format(table)
                self.log(message_if_yes, verbose=verbose)
            return result
        else:
            if message_if_no and '{}' in message_if_no:
                message_if_no = message_if_no.format(table)
            if stop_if_no:
                raise ValueError(message_if_no)
            else:
                if message_if_no:
                    self.log(message_if_no, verbose=verbose)

    def create_table(self, name, schema, drop_if_exists=False, verbose=arg.DEFAULT) -> NoReturn:
        verbose = arg.undefault(verbose, self.verbose)
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
        if drop_if_exists:
            self.drop_table(name, verbose=verbose)
        message = 'Creating table:'
        query = 'CREATE TABLE {name} ({schema});'.format(
            name=name,
            schema=schema_str,
        )
        self.execute(
            query, get_data=False, commit=True,
            verbose=message if verbose is True else verbose,
        )
        self.post_create_action(name, verbose=verbose)
        self.log('Table {name} is created.'.format(name=name), verbose=verbose)

    def post_create_action(self, name, **kwargs):
        pass

    def drop_table(self, name, if_exists=True, verbose=arg.DEFAULT) -> NoReturn:
        self.execute_if_exists(
            query='DROP TABLE IF EXISTS {};',
            table=name,
            message_if_yes='Table {} has been dropped',
            message_if_no='Table {} did not exists before, nothing dropped.',
            stop_if_no=not if_exists,
            verbose=verbose,
        )

    def copy_table(self, old, new, if_exists=False, verbose=arg.DEFAULT) -> NoReturn:
        cat_old, name_old = old.split('.')
        cat_new, name_new = new.split('.') if '.' in new else (cat_old, new)
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

    def rename_table(self, old, new, if_exists=False, verbose=arg.DEFAULT) -> NoReturn:
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

    def select(self, table_name, fields, filters=None, verbose=arg.DEFAULT):
        fields_str = fields if isinstance(fields, str) else ', '.join(fields)
        filters_str = filters if isinstance(filters, str) else ' AND '.join(filters) if filters is not None else ''
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

    def select_count(self, table, verbose=arg.DEFAULT) -> int:
        return self.select(table, fields='COUNT(*)', verbose=verbose)[0][0]

    def select_all(self, table, verbose=arg.DEFAULT):
        return self.select(table, fields='*', verbose=verbose)

    @staticmethod
    def _get_schema_stream_from_data(data: Data, schema: Optional[SchemaInterface] = None, **file_kwargs) -> Stream:
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
            self, table, rows, columns,
            step=DEFAULT_STEP, skip_errors=False,
            expected_count=arg.DEFAULT, return_count=True,
            verbose=arg.DEFAULT,
    ):
        pass

    def insert_schematized_stream(
            self, table: str, stream: Stream,
            skip_errors: bool = False, step: int = DEFAULT_STEP,
            verbose: Union[bool, arg.DefaultArgument] = arg.DEFAULT,
    ) -> Optional[int]:
        columns = stream.get_columns()
        assert columns, 'columns in StructStream must be defined (got {})'.format(stream)
        expected_count = stream.get_count()
        final_count = stream.calc(
            lambda a: self.insert_rows(
                table, rows=a, columns=columns,
                step=step, expected_count=expected_count,
                skip_errors=skip_errors, return_count=True,
                verbose=verbose,
            ),
        )
        return final_count

    def insert_data(
            self,
            table: str, data: Data,
            schema: Union[SchemaInterface, tuple] = tuple(),
            encoding: Optional[str] = None, skip_errors: bool = False,
            skip_lines: Optional[int] = 0, skip_first_line: bool = False,
            step: Union[int, arg.DefaultArgument] = DEFAULT_STEP,
            verbose: Union[bool, arg.DefaultArgument] = arg.DEFAULT,
    ) -> tuple:
        if not arg.is_defined(skip_lines):
            skip_lines = 0
        is_schema_description = isinstance(schema, sh.SchemaDescription) or hasattr(schema, 'get_schema_str')
        if not is_schema_description:
            message = 'Schema as {} is deprecated, use sh.SchemaDescription instead'.format(type(schema))
            self.log(msg=message, level=log.LoggingLevel.Warning)
            schema = sh.SchemaDescription(schema)
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
        final_count = self.insert_schematized_stream(
            table, input_stream,
            skip_errors=skip_errors, step=step,
            verbose=verbose,
        )
        return initial_count, final_count

    def force_upload_table(
            self,
            table: str, schema: SchemaInterface, data: Data,
            encoding=None,
            step=DEFAULT_STEP,
            skip_lines=0, skip_first_line=False, max_error_rate=0.0,
            verbose=arg.DEFAULT,
    ) -> NoReturn:
        verbose = arg.undefault(verbose, self.verbose)
        if not skip_lines:
            self.create_table(table, schema=schema, drop_if_exists=True, verbose=verbose)
        skip_errors = (max_error_rate is None) or (max_error_rate > DEFAULT_ERRORS_THRESHOLD)
        initial_count, write_count = self.insert_data(
            table, schema=schema, data=data,
            encoding=encoding, skip_first_line=skip_first_line,
            step=step, skip_lines=skip_lines, skip_errors=skip_errors,
            verbose=verbose,
        )
        write_count += skip_lines
        result_count = self.select_count(table)
        if write_count:
            error_rate = (write_count - result_count) / write_count
            message = 'Check counts: {} initial, {} uploaded, {} written, {} error_rate'
        else:
            error_rate = 1.0
            message = 'ERR: Data {} and/or able {} is empty.'.format(data, table)
        self.log(message.format(initial_count, write_count, result_count, error_rate), verbose=verbose)
        if max_error_rate is not None:
            message = 'Too many errors or skipped lines ({} > {})'.format(error_rate, max_error_rate)
            assert error_rate < max_error_rate, message

    def safe_upload_table(
            self,
            table, schema, data,
            encoding=None,
            step=DEFAULT_STEP,
            skip_lines=0, skip_first_line=False, max_error_rate=0.0,
            verbose=arg.DEFAULT,
    ):
        tmp_name = '{}_tmp_upload'.format(table)
        bak_name = '{}_bak'.format(table)
        verbose = arg.undefault(verbose, self.verbose)
        self.force_upload_table(
            table=tmp_name, schema=schema, data=data, encoding=encoding, skip_first_line=skip_first_line,
            step=step, skip_lines=skip_lines, max_error_rate=max_error_rate,
            verbose=verbose,
        )
        self.drop_table(bak_name, if_exists=True, verbose=verbose)
        self.rename_table(table, bak_name, if_exists=True, verbose=verbose)
        self.rename_table(tmp_name, table, if_exists=True, verbose=verbose)
