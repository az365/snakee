try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as sm
    from connectors import connector_classes as ct
    from utils import arguments as arg
    from loggers import logger_classes as log
    from items.struct_interface import StructInterface
    from items.flat_struct import FlatStruct
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...streams import stream_classes as sm
    from ...connectors import connector_classes as ct
    from ...utils import arguments as arg
    from ...loggers import logger_classes as log
    from ...items.struct_interface import StructInterface
    from ...items.flat_struct import FlatStruct


class Table(ct.LeafConnector):
    def __init__(
            self,
            name,
            struct,
            database,
            reconnect=True,
            **kwargs
    ):
        super().__init__(
            name=name,
            parent=database,
        )
        self.struct = struct

        if not isinstance(struct, StructInterface):
            message = 'Struct as {} is deprecated. Use items.FlatStruct instead.'.format(type(struct))
            self.log(msg=message, level=log.LoggingLevel.Warning)
        self.meta = kwargs
        if reconnect:
            if hasattr(self.get_database(), 'connect'):
                self.get_database().connect(reconnect=True)
        self.links = list()

    def get_database(self):
        return self.get_parent()

    def get_count(self, verbose=arg.AUTO):
        return self.get_database().select_count(self.get_name(), verbose=verbose)

    def get_struct(self):
        if not self.struct:
            self.set_struct(struct=arg.AUTO)
        return self.struct

    def get_columns(self):
        return self.get_struct().get_columns()

    def get_data(self, verbose=arg.AUTO):
        return self.get_database().select_all(self.get_name(), verbose=verbose)

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

    def to_stream(self, stream_type=arg.AUTO, verbose=arg.AUTO):
        stream_type = arg.acquire(stream_type, sm.RowStream)
        stream = self.get_stream()
        if isinstance(stream_type, sm.RowStream):
            return stream
        elif isinstance(stream_type, sm.RecordStream):
            return stream.to_record_stream(columns=self.get_columns())
        elif isinstance(stream_type, sm.StructStream):
            return stream.structure(self.get_struct(), verbose=verbose)
        else:
            raise ValueError('only RowStream, RecordStream, StructStream is supported for Table connector')

    def from_stream(self, stream, **kwargs):
        assert sm.is_stream(stream)
        self.upload(data=stream, **kwargs)

    def set_struct(self, struct):
        if struct is None:
            self.struct = None
        elif isinstance(struct, StructInterface):
            self.struct = struct
        elif isinstance(struct, (list, tuple)):
            if max([isinstance(f, (list, tuple)) for f in struct]):
                self.struct = FlatStruct(struct)
            else:
                self.struct = FlatStruct.get_struct_detected_by_title_row(struct)
        elif struct == arg.AUTO:
            self.struct = self.get_struct_from_database()
        else:
            message = 'struct must be StructInterface or tuple with fields_description (got {})'.format(type(struct))
            raise TypeError(message)

    def is_existing(self):
        return self.get_database().exists_table(self.get_path())

    def describe(self):
        return self.get_database().describe_table(self.get_path())

    def get_struct_from_database(self, set_struct=False):
        struct = FlatStruct(self.describe())
        if set_struct:
            self.struct = struct
        return struct

    def create(self, drop_if_exists, verbose=arg.AUTO):
        return self.get_database().create_table(
            self.get_name(),
            struct=self.struct,
            drop_if_exists=drop_if_exists,
            verbose=verbose,
        )

    def upload(
            self, data,
            encoding=None, skip_first_line=False,
            skip_lines=0, max_error_rate=0.0,
            verbose=arg.AUTO
    ):
        return self.get_database().safe_upload_table(
            self.get_name(),
            data=data,
            struct=self.struct,
            skip_lines=skip_lines,
            skip_first_line=skip_first_line,
            encoding=encoding,
            max_error_rate=max_error_rate,
            verbose=verbose,
        )
