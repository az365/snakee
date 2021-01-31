from abc import abstractmethod
from enum import Enum

try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as fx
    from connectors import (
        abstract_connector as ac,
        connector_classes as cs,
    )
    from utils import (
        arguments as arg,
        mappers as ms,
    )
    from loggers import logger_classes
    from schema import schema_classes as sh
    from functions import all_functions as fs
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from streams import stream_classes as fx
    from connectors import (
        connector_classes as cs,
        abstract_connector as ac,
    )
    from utils import (
        arguments as arg,
        mappers as ms,
    )
    from loggers import logger_classes
    from schema import schema_classes as sh
    from functions import all_functions as fs


AUTO = arg.DEFAULT
TEST_QUERY = 'SELECT now()'
DEFAULT_GROUP = 'PUBLIC'
DEFAULT_STEP = 1000
DEFAULT_ERRORS_THRESHOLD = 0.05


class DatabaseType(Enum):
    PostgresDatabase = 'pg'
    ClickhouseDatabase = 'ch'


class AbstractDatabase(ac.AbstractStorage):
    def __init__(self, name, host, port, db, user, password, verbose=ac.AUTO, context=None, **kwargs):
        super().__init__(
            name=name,
            context=context,
            verbose=verbose,
        )
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = password
        self.conn_kwargs = kwargs
        self.connection = None

    def get_default_child_class(self):
        return cs.Table

    def get_tables(self):
        return self.get_items()

    def table(self, name, schema=None, **kwargs):
        table = self.get_tables().get(name)
        if table:
            assert not kwargs, 'table connection {} is already registered'.format(name)
        else:
            assert schema is not None, 'for create table schema must be defined'
            table = cs.Table(name, schema=schema, database=self, **kwargs)
            self.get_tables()[name] = table
        return table

    def close(self):
        if hasattr(self, 'disconnect'):
            return self.disconnect()

    def get_links(self):
        for item in self.get_items():
            yield from item.get_links()

    @classmethod
    def need_connection(cls):
        return hasattr(cls, 'connection')

    @abstractmethod
    def get_dialect_name(self):
        pass

    @abstractmethod
    def exists_table(self, name, verbose=arg.DEFAULT):
        pass

    @abstractmethod
    def execute(self, query, get_data=ac.AUTO, commit=ac.AUTO, verbose=arg.DEFAULT):
        pass

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

    def create_table(self, name, schema, drop_if_exists=False, verbose=arg.DEFAULT):
        verbose = arg.undefault(verbose, self.verbose)
        if isinstance(schema, sh.SchemaDescription):
            schema_str = schema.get_schema_str(dialect=self.get_dialect_name())
        elif isinstance(schema, str):
            schema_str = schema
            message = 'String Schemas is deprecated. Use schema.SchemaDescription instead.'
            self.log(msg=message, level=logger_classes.LoggingLevel.Warning)
        else:
            schema_str = ', '.join(['{} {}'.format(c[0], c[1]) for c in schema])
            message = 'Tuple Schemas is deprecated. Use schema.SchemaDescription instead.'
            self.log(msg=message, level=logger_classes.LoggingLevel.Warning)
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

    def drop_table(self, name, if_exists=True, verbose=arg.DEFAULT):
        self.execute_if_exists(
            query='DROP TABLE IF EXISTS {};',
            table=name,
            message_if_yes='Table {} has been dropped',
            message_if_no='Table {} did not exists before, nothing dropped.',
            stop_if_no=not if_exists,
            verbose=verbose,
        )

    def copy_table(self, old, new, if_exists=False, verbose=arg.DEFAULT):
        cat_old, name_old = old.split('.')
        cat_new, name_new = new.split('.') if '.' in new else cat_old, new
        assert cat_new == cat_old, 'Can copy within same scheme (folder) only'
        new = name_new
        self.execute_if_exists(
            query='CREATE TABLE {new} AS TABLE {old};'.format(new=new, old=old),
            table=old,
            message_if_yes='Table {old} is copied to {new}'.format(old=old, new=new),
            message_if_no='Can not copy table {}: not exists',
            stop_if_no=not if_exists,
            verbose=verbose,
        )

    def rename_table(self, old, new, if_exists=False, verbose=arg.DEFAULT):
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

    def select_count(self, table, verbose=arg.DEFAULT):
        return self.select(table, fields='COUNT(*)', verbose=verbose)[0][0]

    def select_all(self, table, verbose=arg.DEFAULT):
        return self.select(table, fields='*', verbose=verbose)

    @abstractmethod
    def insert_rows(
            self, table, rows, columns,
            step=DEFAULT_STEP, skip_errors=False,
            expected_count=arg.DEFAULT, return_count=True,
            verbose=arg.DEFAULT,
    ):
        pass

    def insert_schematized_flux(self, table, flux, skip_errors=False, step=DEFAULT_STEP, verbose=arg.DEFAULT):
        columns = flux.get_columns()
        expected_count = flux.count
        final_count = flux.calc(
            lambda a: self.insert_rows(
                table, rows=a, columns=columns,
                step=step, expected_count=expected_count,
                skip_errors=skip_errors, return_count=True,
                verbose=verbose,
            ),
        )
        return final_count

    def insert_data(
            self, table, data, schema=tuple(),
            encoding=None, skip_first_line=False,
            skip_lines=0, skip_errors=False, step=DEFAULT_STEP,
            verbose=arg.DEFAULT,
    ):
        if not isinstance(schema, sh.SchemaDescription):
            message = 'Schema as {} is deprecated, use sh.SchemaDescription instead'.format(type(schema))
            self.log(msg=message, level=logger_classes.LoggingLevel.Warning)
            schema = sh.SchemaDescription(schema)
        if fx.is_flux(data):
            fx_input = data
        elif cs.is_file(data):
            fx_input = data.to_schema_flux()
            assert fx_input.get_columns() == schema.get_columns()
        elif isinstance(data, str):
            fx_input = fx.RowsFlux.from_csv_file(
                filename=data,
                encoding=encoding,
                skip_first_line=skip_first_line,
                verbose=verbose,
            )
        else:
            fx_input = fx.AnyFlux(data)
        if skip_lines:
            fx_input = fx_input.skip(skip_lines)
        if fx_input.flux_type() != fx.FluxType.SchemaFlux:
            fx_input = fx_input.schematize(
                schema,
                skip_bad_rows=True,
                verbose=True,
            ).update_meta(
                count=fx_input.count,
            )
        initial_count = fx_input.count + skip_lines
        final_count = self.insert_schematized_flux(
            table, fx_input,
            skip_errors=skip_errors, step=step,
            verbose=verbose,
        )
        return initial_count, final_count

    def force_upload_table(
            self,
            table, schema, data,
            encoding=None,
            step=DEFAULT_STEP,
            skip_lines=0, skip_first_line=False, max_error_rate=0.0,
            verbose=arg.DEFAULT,
    ):
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
        error_rate = (write_count - result_count) / write_count
        message = 'Check counts: {} initial, {} uploaded, {} written, {} error_rate'
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
