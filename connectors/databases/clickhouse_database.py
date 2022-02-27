from typing import Optional, Iterable, Union
import requests

try:  # Assume we're a submodule in a package.
    from interfaces import (
        ConnectorInterface,
        ConnType, DialectType, LoggingLevel,
        AUTO, Auto, AutoBool, AutoContext, AutoName, Name, Count, Array, ARRAY_TYPES,
    )
    from base.functions.arguments import get_name
    from connectors.databases.abstract_database import AbstractDatabase, TEST_QUERY, DEFAULT_STEP
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        ConnectorInterface,
        ConnType, DialectType, LoggingLevel,
        AUTO, Auto, AutoBool, AutoContext, AutoName, Name, Count, Array, ARRAY_TYPES,
    )
    from base.functions.arguments import get_name
    from .abstract_database import AbstractDatabase, TEST_QUERY, DEFAULT_STEP


class ClickhouseDatabase(AbstractDatabase):
    def __init__(
            self,
            name: Name,
            host: Name = 'localhost',
            port: int = 8443,
            db: Name = 'public',
            user: AutoName = AUTO,
            password: AutoName = AUTO,
            context: AutoContext = AUTO,
            **kwargs
    ):
        super().__init__(
            name=name,
            host=host, port=port, db=db,
            user=user, password=password,
            context=context,
            **kwargs
        )

    @classmethod
    def get_dialect_type(cls) -> DialectType:
        return DialectType.Clickhouse

    def execute(
            self,
            query: str = TEST_QUERY,
            get_data: AutoBool = AUTO,
            commit: AutoBool = AUTO,
            verbose: bool = True,
    ) -> Optional[Iterable]:
        url = 'https://{host}:{port}/?database={db}&query={query}'.format(
            host=self.host,
            port=self.port,
            db=self.db,
            query=query,
        )
        auth = {
            'X-ClickHouse-User': self.user,
            'X-ClickHouse-Key': self.password,
        }
        request_props = {'headers': auth}
        cert_filename = self.conn_kwargs.get('cert_filename') or self.conn_kwargs.get('verify')
        if cert_filename:
            request_props['verify'] = cert_filename
        self.log('Execute query: {}'. format(query), verbose=verbose)
        res = requests.get(
            url,
            **request_props
        )
        res.raise_for_status()
        if get_data:
            return res.text

    def exists_table(self, name: Name, verbose: AutoBool = AUTO):
        query = 'EXISTS TABLE {}'.format(name)
        answer = self.execute(query, verbose=verbose)
        return answer[0] == '1'

    def describe_table(self, name: Name, output_format: Optional[str] = None, verbose: AutoBool = AUTO):
        query = 'DESCRIBE TABLE {table}'.format(table=self.get_path())
        if output_format:
            query = '{} FORMAT {}'.format(query, output_format)
        return self.execute(query, verbose=verbose)

    def insert_rows(
            self,
            table: Union[Name, ConnectorInterface],
            rows: Array,
            columns: Array,
            step: Count = DEFAULT_STEP,
            skip_errors: bool = False,
            expected_count: Count = None,
            return_count: bool = True,
            verbose: AutoBool = AUTO,
    ):
        verbose = Auto.delayed_acquire(verbose, self.is_verbose)
        table_name = get_name(table)
        count = len(rows) if isinstance(rows, ARRAY_TYPES) else expected_count
        if count == 0:
            message = 'Rows are empty, nothing to insert into {}.'.format(table)
            if skip_errors:
                self.log(message, verbose=verbose)
            else:
                raise ValueError(message)
        query_template = 'INSERT INTO {table} ({columns}) VALUES ({values})'.format(
            table=table_name,
            columns=', '.join(columns),
            values='{}',
        )
        message = verbose if isinstance(verbose, str) else 'Inserting into {table}'.format(table=table_name)
        progress = self.get_new_progress(message, count=count, context=self.get_context())
        progress.start()
        n = 0
        for n, row in enumerate(rows):
            values = ', '.format(row)
            cur_query = query_template.format(values)
            if skip_errors:
                try:
                    self.execute(cur_query)
                except requests.RequestException as e:
                    self.log(['Error line:', str(row)], level=LoggingLevel.Debug, verbose=verbose)
                    self.log([e.__class__.__name__, e], level=LoggingLevel.Error)
            else:
                self.execute(cur_query)
            if (n + 1) % step == 0:
                progress.update(n)
        progress.finish(n)
        if return_count:
            return n


ConnType.add_classes(ClickhouseDatabase)
