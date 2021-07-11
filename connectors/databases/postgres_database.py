from typing import Optional, Union, Iterable, NoReturn
import gc
import psycopg2
import psycopg2.extras

try:  # Assume we're a sub-module in a package.
    from connectors.databases import abstract_database as ad
    from utils import (
        arguments as arg,
        mappers as ms,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..databases import abstract_database as ad
    from ...utils import (
        arguments as arg,
        mappers as ms,
    )


class PostgresDatabase(ad.AbstractDatabase):
    def __init__(
            self,
            name: str, host: str, port: int, db: str,
            user: Optional[str] = None, password: Optional[str] = None,
            context=arg.DEFAULT,
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

    def get_connection(self, connect=False):
        if connect and not self.connection:
            self.connect()
        return self.connection

    def connect(self, reconnect=True):
        if self.is_connected() and reconnect:
            self.disconnect(True)
        if not self.is_connected():
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.db,
                user=self.user,
                password=self.password,
                **self.conn_kwargs
            )
        return self.connection

    def disconnect(self, skip_errors=False, verbose=arg.DEFAULT) -> Optional[int]:
        verbose = arg.undefault(verbose, self.verbose)
        if self.is_connected():
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

    def execute(self, query=ad.TEST_QUERY, get_data=arg.DEFAULT, commit=arg.DEFAULT, data=None, verbose=arg.DEFAULT):
        verbose = arg.undefault(verbose, self.verbose)
        message = verbose if isinstance(verbose, str) else 'Execute:'
        level = self.LoggingLevel.Debug
        self.log([message, ms.remove_extra_spaces(query)], level=level, end='\r', verbose=verbose)
        if get_data == arg.DEFAULT:
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
        self.log([message, 'successful'], end='\r', verbose=bool(verbose))
        if get_data:
            return result

    def execute_batch(self, query, batch, step=ad.DEFAULT_STEP, cursor=arg.DEFAULT) -> NoReturn:
        if cursor == arg.DEFAULT:
            cursor = self.connect().cursor()
        psycopg2.extras.execute_batch(cursor, query, batch, page_size=step)

    def grant_permission(self, name, permission='SELECT', group=ad.DEFAULT_GROUP, verbose=arg.DEFAULT) -> NoReturn:
        verbose = arg.undefault(verbose, self.verbose)
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

    def post_create_action(self, name, verbose=arg.DEFAULT) -> NoReturn:
        self.grant_permission(name, verbose=verbose)

    def exists_table(self, name, verbose=arg.DEFAULT) -> bool:
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

    def describe_table(self, name, verbose=arg.DEFAULT):
        return self.select(
            table='information_schema.COLUMNS',
            fields=['COLUMN_NAME', 'DATA_TYPE'],
            filters=["TABLE_NAME = '{table}'".format(table=name)],
        )

    def insert_rows(
            self,
            table: str, rows: Iterable, columns: Iterable,
            step: int = ad.DEFAULT_STEP, skip_errors: bool = False,
            expected_count: Optional[int] = None, return_count: bool = True,
            verbose: Union[bool, arg.DefaultArgument] = arg.DEFAULT,
    ) -> Optional[int]:
        assert columns, 'columns must be defined'
        verbose = arg.undefault(verbose, self.verbose)
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
                    self.log(['Error line:', str(row)], level=self.LoggingLevel.Debug, verbose=verbose)
                    self.log([e.__class__.__name__, e], level=self.LoggingLevel.Error)
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
