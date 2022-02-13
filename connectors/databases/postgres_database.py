from typing import Optional, Iterable, Sized
import gc

try:  # Assume we're a submodule in a package.
    from interfaces import (
        ConnType, DialectType, LoggingLevel,
        AUTO, Auto, AutoBool, Count, Array, ARRAY_TYPES,
    )
    from utils.external import psycopg2
    from connectors.databases.abstract_database import AbstractDatabase, TEST_QUERY, DEFAULT_STEP, DEFAULT_GROUP
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        ConnType, DialectType, LoggingLevel,
        AUTO, Auto, AutoBool, Count, Array, ARRAY_TYPES,
    )
    from ...utils.external import psycopg2
    from ..databases.abstract_database import AbstractDatabase, TEST_QUERY, DEFAULT_STEP, DEFAULT_GROUP


class PostgresDatabase(AbstractDatabase):
    def __init__(
            self,
            name: str, host: str, port: int, db: str,
            user: Optional[str] = None, password: Optional[str] = None,
            context=AUTO,
            **kwargs
    ):
        super().__init__(
            name=name, host=host, port=port, db=db,
            user=user, password=password,
            context=context,
            **kwargs
        )

    @classmethod
    def get_dialect_type(cls) -> DialectType:
        return DialectType.Postgres

    def is_connected(self) -> bool:
        return (self.connection is not None) and not self.connection.closed

    def get_connection(self, connect: bool = False):
        if connect and not self.connection:
            self.connect()
        return self.connection

    def connect(self, reconnect: bool = True):
        if self.is_connected() and reconnect:
            self.disconnect(True)
        if not self.is_connected():
            if not psycopg2:
                raise ImportError('psycopg2 must be installed (pip install psycopg2)')
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.db,
                user=self.user,
                password=self.password,
                **self.conn_kwargs
            )
        return self.connection

    def disconnect(self, skip_errors: bool = False, verbose=AUTO) -> Count:
        verbose = Auto.acquire(verbose, self.verbose)
        if self.is_connected():
            if not psycopg2:
                raise ImportError('psycopg2 must be installed (pip install psycopg2)')
            if skip_errors:
                try:
                    self.connection.close()
                except psycopg2.OperationalError:
                    message = 'Connection to {} already closed.'.format(self.host)
                    self.log(message, level=LoggingLevel.Warning, verbose=verbose)
            else:
                self.connection.close()
            self.connection = None
            return 1

    def execute(
            self,
            query: str = TEST_QUERY,
            get_data: AutoBool = AUTO,
            commit: AutoBool = AUTO,
            data: Optional[Iterable] = None,
            verbose: AutoBool = AUTO,
    ):
        verbose = Auto.acquire(verbose, self.verbose)
        message = verbose if isinstance(verbose, str) else 'Execute: {}'
        if '{}' in message:
            message = message.format(self._get_compact_query_view(query))
        self.log(message, level=LoggingLevel.Debug, end='\r', verbose=verbose)
        if get_data == AUTO:
            if 'SELECT' in query and 'GRANT' not in query:
                get_data, commit = True, False
            else:
                get_data, commit = False, True
        has_connection = self.is_connected()
        cur = self.connect(reconnect=False).cursor()
        if data:
            cur.execute(query, data)
        else:
            cur.execute(query)
        if get_data:
            result = cur.fetchall()
        else:
            result = None
        if commit:
            self.get_connection().commit()
        cur.close()
        if not has_connection:
            self.connection.close()
        self.log('{} {}'.format(message, 'successful'), end='\r', verbose=bool(verbose))
        if get_data:
            return result

    def execute_batch(self, query: str, batch: Iterable, step: int = DEFAULT_STEP, cursor=AUTO) -> None:
        if cursor == AUTO:
            cursor = self.connect().cursor()
        if not psycopg2:
            raise ImportError('psycopg2 must be installed (pip install psycopg2)')
        psycopg2.extras.execute_batch(cursor, query, batch, page_size=step)

    def grant_permission(self, name: str, permission='SELECT', group=DEFAULT_GROUP, verbose: AutoBool = AUTO) -> None:
        verbose = Auto.acquire(verbose, self.verbose)
        message = 'Grant access:'
        query = 'GRANT {permission} ON {name} TO {group};'.format(
            name=name,
            permission=permission,
            group=group,
        )
        self.execute(
            query, get_data=False, commit=True,
            verbose=message if verbose is True else verbose,
        )

    def post_create_action(self, name: str, verbose=AUTO) -> None:
        self.grant_permission(name, verbose=verbose)

    def exists_table(self, name: str, verbose=AUTO) -> bool:
        schema_name, table_name = self._get_schema_and_table_name(name, default_schema='public')
        template = """
            SELECT 1
            FROM   pg_catalog.pg_class c
            JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE  n.nspname = '{schema}'
            AND    c.relname = '{table}'
        """
        query = template.format(schema=schema_name, table=table_name)
        return bool(self.execute(query, verbose=verbose))

    def describe_table(self, name: str, verbose: AutoBool = AUTO):
        schema_name, table_name = self._get_schema_and_table_name(name)
        filters = ["TABLE_NAME = '{table}'".format(table=table_name)]
        if schema_name:
            filters = ["TABLE_SCHEMA = '{schema}'".format(schema=schema_name)] + filters
        response = self.execute_select(
            table='information_schema.COLUMNS',
            fields=['COLUMN_NAME', 'DATA_TYPE'],
            filters=filters,
            sort=['ordinal_position'],
            verbose=verbose,
        )
        return response

    def insert_rows(
            self,
            table: str, rows: Iterable, columns: Array,
            step: int = DEFAULT_STEP, skip_errors: bool = False,
            expected_count: Count = None, return_count: bool = True,
            verbose: AutoBool = AUTO,
    ) -> Count:
        assert isinstance(columns, ARRAY_TYPES), 'list or tuple expected, got {}'.format(columns)
        verbose = Auto.acquire(verbose, self.verbose)
        if isinstance(rows, Sized):
            count = len(rows)
        else:
            count = expected_count
        conn = self.connect(reconnect=True)
        cur = conn.cursor()
        use_fast_batch_method = not skip_errors
        query_args = dict(table=table)
        if use_fast_batch_method:
            query_template = 'INSERT INTO {table} VALUES ({values});'
            placeholders = ['%({})s'.format(c) for c in columns]
        else:  # elif skip_errors:
            query_template = 'INSERT INTO {table} ({columns}) VALUES ({values})'
            placeholders = ['%s' for _ in columns]
            query_args['columns'] = ', '.join(columns)
        query_args['values'] = ', '.join(placeholders)
        query = query_template.format(**query_args)
        message = verbose if isinstance(verbose, str) else 'Commit {}b to {}'.format(step, table)
        progress = self.get_new_progress(message, count=count)
        progress.start()
        records_batch = list()
        n = 0
        for n, row in enumerate(rows):
            if use_fast_batch_method:
                current_record = {k: v for k, v in zip(columns, row)}
                records_batch.append(current_record)
            elif skip_errors:
                try:
                    cur.execute(query, row)
                except TypeError or IndexError as e:  # TypeError: not all arguments converted during string formatting
                    self.log('Error line: {}'.format(str(row)), level=LoggingLevel.Debug, verbose=verbose)
                    self.log('{}: {}'.format(e.__class__.__name__, e), level=LoggingLevel.Error)
            if (n + 1) % step == 0:
                if use_fast_batch_method:
                    self.execute_batch(query, records_batch, step, cursor=cur)
                    records_batch = list()
                if not progress.get_position():
                    progress.update(0)
                conn.commit()
                progress.update(n)
                gc.collect()
        if use_fast_batch_method:
            self.execute_batch(query, records_batch, step, cursor=cur)
        conn.commit()
        progress.finish(n)
        if return_count:
            return n


ConnType.add_classes(PostgresDatabase)
