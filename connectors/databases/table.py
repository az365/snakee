try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as sm
    from connectors import connector_classes as ct
    from utils import arguments as arg
    from loggers import logger_classes
    from schema import schema_classes as sh
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...streams import stream_classes as sm
    from ...connectors import connector_classes as ct
    from ...utils import arguments as arg
    from ...loggers import logger_classes
    from ...schema import schema_classes as sh


class Table(ct.LeafConnector):
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
        return self.get_database().select_count(self.name, verbose=verbose)

    def get_schema(self):
        if not self.schema:
            self.set_schema(schema=arg.DEFAULT)
        return self.schema

    def get_columns(self):
        return self.get_schema().get_columns()

    def get_data(self, verbose=arg.DEFAULT):
        return self.get_database().select_all(self.name, verbose=verbose)

    def get_stream(self):
        count = self.get_count()
        stream = sm.RowStream(
            self.get_data(),
            count=count,
            # source=self,
            context=self.get_context(),
        )
        self.links.append(stream)
        return stream

    def to_stream(self, stream_type=arg.DEFAULT, verbose=arg.DEFAULT):
        stream_type = arg.undefault(stream_type, sm.RowStream)
        stream = self.get_stream()
        if isinstance(stream_type, sm.RowStream):
            return stream
        elif isinstance(stream_type, sm.RecordStream):
            return stream.to_record_stream(columns=self.get_columns())
        elif isinstance(stream_type, sm.SchemaStream):
            return stream.schematize(self.get_schema(), verbose=verbose)
        else:
            raise ValueError('only RowStream, RecordStream, SchemaStream is supported for Table connector')

    def from_stream(self, stream, **kwargs):
        assert sm.is_stream(stream)
        self.upload(data=stream, **kwargs)

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
        elif schema == arg.DEFAULT:
            self.schema = self.get_schema_from_database()
        else:
            message = 'schema must be SchemaDescription or tuple with fields_description (got {})'.format(type(schema))
            raise TypeError(message)

    def is_existing(self):
        return self.get_database().exists_table(self.get_path())

    def describe(self):
        return self.get_database().describe_table(self.get_path())

    def get_schema_from_database(self, set_schema=False):
        schema = sh.SchemaDescription(self.describe())
        if set_schema:
            self.schema = schema
        return schema

    def create(self, drop_if_exists, verbose=arg.DEFAULT):
        return self.get_database().create_table(
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
        return self.get_database().safe_upload_table(
            self.name,
            data=data,
            schema=self.schema,
            skip_lines=skip_lines,
            skip_first_line=skip_first_line,
            encoding=encoding,
            max_error_rate=max_error_rate,
            verbose=verbose,
        )
