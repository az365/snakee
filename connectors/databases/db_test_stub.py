from typing import Optional, Iterable

try:  # Assume we're a submodule in a package.
    from connectors.databases.postgres_database import PostgresDatabase, TEST_QUERY, AUTO, Auto, AutoBool
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import
    from ..databases.postgres_database import PostgresDatabase, TEST_QUERY, AUTO, Auto, AutoBool


class DatabaseTestStub(PostgresDatabase):
    def __init__(
            self,
            name: str, host: str, port: int, db: str,
            user: Optional[str] = None, password: Optional[str] = None,
            context=AUTO,
            **kwargs
    ):
        self.test_stub_response = None
        super().__init__(
            name=name, host=host, port=port, db=db,
            user=user, password=password,
            context=context,
            **kwargs
        )

    def execute(
            self,
            query: str = TEST_QUERY,
            get_data: AutoBool = AUTO,
            commit: AutoBool = AUTO,
            data: Optional[Iterable] = None,
            verbose: AutoBool = AUTO,
    ):
        query = self._get_compact_query_view(query)
        if query.startswith('SELECT'):
            return self.test_stub_response
        else:
            raise NotImplementedError('Received query: {query}'.format(query=query))
