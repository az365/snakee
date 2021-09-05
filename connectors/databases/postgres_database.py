from typing import Optional, Union, Iterable, NoReturn
import gc

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from utils.external import psycopg2
    from connectors.databases.abstract_database import AbstractDatabase, TEST_QUERY, DEFAULT_STEP, DEFAULT_GROUP
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...utils.external import psycopg2
    from ..databases.abstract_database import AbstractDatabase, TEST_QUERY, DEFAULT_STEP, DEFAULT_GROUP


class PostgresDatabase(AbstractDatabase):
    def __init__(
            self,
            name: str, host: str, port: int, db: str,
            user: Optional[str] = None, password: Optional[str] = None,
            context=arg.AUTO,
            **kwargs
    ):
        super().__init__(
            name=name, host=host, port=port, db=db,
            user=user, password=password,
            context=context,
            **kwargs
        )

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

    def disconnect(self, skip_errors: bool = False, verbose=arg.AUTO) -> Optional[int]:
        verbose = arg.acquire(verbose, self.verbose)
        if self.is_connected():
            if not psycopg2:
                raise ImportError('psycopg2 must be installed (pip install psycopg2)')
            if skip_errors:
                try:
                    self.connection.close()
                except psycopg2.OperationalError:
                    message = 'Connection to {} already closed.'.format(self.host)
                    self.log(message, level=self.LoggingLevel.Warning, verbose=verbose)
            else:
                self.connection.close()
            self.connection = None
            return 1

    def execute(self, query=TEST_QUERY, get_data=arg.AUTO, commit=arg.AUTO, data=None, verbose=arg.AUTO):
        verbose = arg.acquire(verbose, self.verbose)
        message = verbose if isinstance(verbose, str) else 'Execute: {}'
        if '{}' in message:
            message = message.format(self._get_compact_query_view(query))
        level = self.LoggingLevel.Debug
        self.log(message, level=level, end='\r', verbose=verbose)
        if get_data == arg.AUTO:
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

    def execute_batch(self, query: str, batch: Iterable, step: int = DEFAULT_STEP, cursor=arg.AUTO) -> NoReturn:
        if cursor == arg.AUTO:
            cursor = self.connect().cursor()
        if not psycopg2:
            raise ImportError('psycopg2 must be installed (pip install psycopg2)')
        psycopg2.extras.execute_batch(cursor, query, batch, page_size=step)

    def grant_permission(self, name, permission='SELECT', group=DEFAULT_GROUP, verbose=arg.AUTO) -> NoReturn:
        verbose = arg.acquire(verbose, self.verbose)
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

    def post_create_action(self, name, verbose=arg.AUTO) -> NoReturn:
        self.grant_permission(name, verbose=verbose)

    def exists_table(self, name, verbose=arg.AUTO) -> bool:
        schema, table = name.split('.')
        query = """
            SELECT 1
            FROM   pg_catalog.pg_class c
            JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE  n.nspname = '{schema}'
            AND    c.relname = '{table}'
            AND    c.relkind = 'r'
        """.format(schema=schema, table=table)
        return bool(self.execute(query, verbose))

    def describe_table(self, name, verbose=arg.AUTO):
        return self.select(
            table='information_schema.COLUMNS',
            fields=['COLUMN_NAME', 'DATA_TYPE'],
            filters=["TABLE_NAME = '{table}'".format(table=name)],
        )

    def insert_rows(
            self,
            table: str, rows: Iterable, columns: Iterable,
            step: int = DEFAULT_STEP, skip_errors: bool = False,
            expected_count: Optional[int] = None, return_count: bool = True,
            verbose: Union[bool, arg.Auto] = arg.AUTO,
    ) -> Optional[int]:
        assert columns, 'columns must be defined'
        verbose = arg.acquire(verbose, self.verbose)
        count = len(rows) if isinstance(rows, (list, tuple)) else expected_count
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
                    self.log('Error line: {}'.format(str(row)), level=self.LoggingLevel.Debug, verbose=verbose)
                    self.log('{}: {}'.format(e.__class__.__name__, e), level=self.LoggingLevel.Error)
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
