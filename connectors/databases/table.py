try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as fx
    from connectors import abstract_connector as ac
    from utils import arguments as arg
    from loggers import logger_classes
    from schema import schema_classes as sh
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...streams import stream_classes as fx
    from .. import abstract_connector as ac
    from ...utils import arguments as arg
    from ...loggers import logger_classes
    from ...schema import schema_classes as sh


class Table(ac.LeafConnector):
    def __init__(
            self,
            name,
            schema,
            database,
            reconnect=True,
            **kwargs
    ):
        super().__init__(
            name=name,
            parent=database,
        )
        self.schema = schema
        if not isinstance(schema, sh.SchemaDescription):
            message = 'Schema as {} is deprecated. Use schema.SchemaDescription instead.'.format(type(schema))
            self.log(msg=message, level=logger_classes.LoggingLevel.Warning)
        self.meta = kwargs
        if reconnect:
            if hasattr(self.get_database(), 'connect'):
                self.get_database().connect(reconnect=True)
        self.links = list()

    def get_database(self):
        return self.parent

    def get_count(self, verbose=arg.DEFAULT):
        return self.database.select_count(self.name, verbose=verbose)

    def get_data(self, verbose=arg.DEFAULT):
        return self.database.select_all(self.name, verbose=verbose)

    def get_flux(self):
        count = self.get_count()
        flux = fx.RowsFlux(
            self.get_data(),
            count=count,
            # source=self,
            context=self.get_context(),
        )
        self.links.append(flux)
        return flux

    def set_schema(self, schema):
        if schema is None:
            self.schema = None
        elif isinstance(schema, sh.SchemaDescription):
            self.schema = schema
        elif isinstance(schema, (list, tuple)):
            if max([isinstance(f, (list, tuple)) for f in schema]):
                self.schema = sh.SchemaDescription(schema)
            else:
                self.schema = sh.detect_schema_by_title_row(schema)
        elif schema == ac.AUTO:
            if self.first_line_is_title:
                self.schema = self.detect_schema_by_title_row()
            else:
                self.schema = None
        else:
            message = 'schema must be SchemaDescription or tuple with fields_description (got {})'.format(type(schema))
            raise TypeError(message)

    def create(self, drop_if_exists, verbose=arg.DEFAULT):
        return self.database.create_table(
            self.name,
            schema=self.schema,
            drop_if_exists=drop_if_exists,
            verbose=verbose,
        )

    def upload(
            self, data,
            encoding=None, skip_first_line=False,
            skip_lines=0, max_error_rate=0.0,
            verbose=arg.DEFAULT
    ):
        return self.database.safe_upload_table(
            self.name,
            data=data,
            schema=self.schema,
            skip_lines=skip_lines,
            skip_first_line=skip_first_line,
            encoding=encoding,
            max_error_rate=max_error_rate,
            verbose=verbose,
        )
