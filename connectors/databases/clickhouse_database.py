import requests

try:  # Assume we're a sub-module in a package.
    from connectors.databases import abstract_database as ad
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..databases import abstract_database as ad
    from ...utils import arguments as arg


class ClickhouseDatabase(ad.AbstractDatabase):
    def __init__(
            self,
            name,
            host='localhost',
            port=8443,
            db='public',
            user=arg.DEFAULT,
            password=arg.DEFAULT,
            context=arg.DEFAULT,
            **kwargs
    ):
        super().__init__(
            name=name,
            host=host,
            port=port,
            db=db,
            user=user,
            password=password,
            context=context,
            **kwargs
        )

    def execute(self, query=ad.TEST_QUERY, get_data=ad.AUTO, commit=ad.AUTO, verbose=True):
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

    def exists_table(self, name, verbose=arg.DEFAULT):
        query = 'EXISTS TABLE {}'.format(name)
        answer = self.execute(query, verbose=verbose)
        return answer[0] == '1'

    def describe_table(self, name, output_format=None, verbose=arg.DEFAULT):
        query = 'DESCRIBE TABLE {table}'.format(table=self.get_path())
        if output_format:
            query = '{} FORMAT {}'.format(query, output_format)
        return self.execute(query, verbose=verbose)

    def insert_rows(
            self, table, rows, columns,
            step=ad.DEFAULT_STEP, skip_errors=False,
            expected_count=None, return_count=True,
            verbose=arg.DEFAULT,
    ):
        verbose = arg.undefault(verbose, self.verbose)
        count = len(rows) if isinstance(rows, (list, tuple)) else expected_count
        if count == 0:
            message = 'Rows are empty, nothing to insert into {}.'.format(table)
            if skip_errors:
                self.log(message, verbose=verbose)
            else:
                raise ValueError(message)
        query_template = 'INSERT INTO {table} ({columns}) VALUES ({values})'.format(
            table=table,
            columns=', '.join(columns),
            values='{}',
        )
        message = verbose if isinstance(verbose, str) else 'Inserting into {table}'.format(table=table)
        progress = self.new_progress(message, count=count, verbose=verbose)
        progress.start()
        n = 0
        for n, row in enumerate(rows):
            values = ', '.format(row)
            cur_query = query_template.format(values)
            if skip_errors:
                try:
                    self.execute(cur_query)
                except requests.RequestException as e:
                    self.log(['Error line:', str(row)], level=self.LoggingLevel.Debug, verbose=verbose)
                    self.log([e.__class__.__name__, e], level=self.LoggingLevel.Error)
            else:
                self.execute(cur_query)
            if (n + 1) % step == 0:
                progress.update(n)
        progress.finish(n)
        if return_count:
            return n
