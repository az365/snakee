from typing import Optional, Iterable, Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from interfaces import (
        ConnectorInterface, StructInterface, ColumnarInterface, RegularStream,
        ContentFormatInterface, ContentType, StreamType, LoggingLevel,
        ARRAY_TYPES, AUTO, Auto, AutoBool, Count, Name,
    )
    from items.flat_struct import FlatStruct
    from connectors import connector_classes as ct
    from streams import stream_classes as sm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...interfaces import (
        ConnectorInterface, StructInterface, ColumnarInterface, RegularStream,
        ContentFormatInterface, ContentType, StreamType, LoggingLevel,
        ARRAY_TYPES, AUTO, Auto, AutoBool, Count, Name,
    )
    from ...items.flat_struct import FlatStruct
    from .. import connector_classes as ct
    from ...streams import stream_classes as sm

Native = ct.LeafConnector
Stream = Union[RegularStream, ColumnarInterface]
GeneralizedStruct = Union[StructInterface, list, tuple, Auto, None]


class Table(ct.LeafConnector):
    def __init__(
            self,
            name: Name,
            struct: Union[StructInterface, Auto],
            database: ConnectorInterface,
            reconnect: bool = True,
            verbose: AutoBool = AUTO,
    ):
        assert isinstance(database, ct.AbstractDatabase), '*Database expected, got {}'.format(database)
        self.struct = struct
        super().__init__(
            name=name,
            struct=struct,
            parent=database,
            verbose=verbose,
        )
        if reconnect and hasattr(database, 'connect'):
            database.connect(reconnect=True)

    def get_content_type(self) -> ContentType:
        return ContentType.TsvFile

    @staticmethod
    def _get_detected_format_by_name(name: str) -> ContentFormatInterface:
        return ContentType.TsvFile

    def _get_detected_struct(self, set_struct: bool = False, verbose: AutoBool = AUTO) -> StructInterface:
        struct = self.get_struct_from_database(set_struct=set_struct)
        if not isinstance(struct, StructInterface) and arg.delayed_acquire(verbose, self.is_verbose):
            message = 'Struct as {} is deprecated. Use items.FlatStruct instead.'.format(type(struct))
            self.log(msg=message, level=LoggingLevel.Warning)
        return struct

    def get_database(self) -> ConnectorInterface:
        database = self.get_parent()
        assert isinstance(database, ct.AbstractDatabase)
        return database

    def get_count(self, verbose: AutoBool = AUTO) -> Count:
        database = self.get_database()
        assert isinstance(database, ct.AbstractDatabase)
        return database.select_count(self.get_name(), verbose=verbose)

    def get_columns(self) -> list:
        return self.get_struct().get_columns()

    def set_struct(self, struct: GeneralizedStruct, inplace: bool) -> Optional[Native]:
        if struct is None:
            self.struct = None
        elif isinstance(struct, StructInterface):
            self.struct = struct
        elif isinstance(struct, ARRAY_TYPES):
            if max([isinstance(f, ARRAY_TYPES) for f in struct]):
                self.struct = FlatStruct(struct)
            else:
                self.struct = FlatStruct.get_struct_detected_by_title_row(struct)
        elif struct == arg.AUTO:
            self.struct = self.get_struct_from_database()
        else:
            message = 'struct must be StructInterface or tuple with fields_description (got {})'.format(type(struct))
            raise TypeError(message)
        if not inplace:
            return self

    def get_struct(self) -> StructInterface:
        if not self.struct:
            self.set_struct(struct=arg.AUTO)
        return self.struct

    def get_struct_from_database(self, set_struct: bool = False, skip_missing: bool = False) -> StructInterface:
        struct = FlatStruct(self.describe())
        if struct.is_empty() and not skip_missing:
            raise ValueError('Can not get struct for non-existing table {}'.format(self))
        if set_struct:
            self.set_struct(struct)
        return struct

    def get_first_line(self, close: bool = True, verbose: bool = True) -> Optional[str]:
        database = self.get_database()
        assert isinstance(database, ct.AbstractDatabase)
        iter_lines = database.select(self.get_name(), '*', count=1, verbose=verbose)
        lines = list(iter_lines)
        if close:
            self.close()
        if lines:
            return lines[0]

    def get_data(self, verbose: AutoBool = arg.AUTO):
        database = self.get_database()
        assert isinstance(database, ct.AbstractDatabase)
        return database.select_all(self.get_name(), verbose=verbose)

    def get_stream(self) -> Stream:
        stream = sm.RowStream(
            self.get_data(),
            count=self.get_count(),
            context=self.get_context(),
        )
        self.add_child(stream)
        return stream

    def to_stream(self, stream_type: StreamType = arg.AUTO, verbose: AutoBool = arg.AUTO) -> Stream:
        stream_type = arg.acquire(stream_type, sm.RowStream)
        stream = self.get_stream()
        assert isinstance(stream, sm.RowStream)
        if not isinstance(stream_type, StreamType):
            stream_type = StreamType.detect(stream_type)
        if stream_type == StreamType.RowStream:
            return stream
        elif stream_type == StreamType.RecordStream:
            return stream.to_record_stream(columns=self.get_columns())
        elif stream_type == StreamType.StructStream:
            return stream.structure(self.get_struct(), verbose=verbose)
        else:
            msg = 'only RowStream, RecordStream, StructStream is supported for Table connector, got {}'
            raise ValueError(msg.format(stream_type))

    def from_stream(self, stream, **kwargs):
        assert isinstance(stream, RegularStream)
        self.upload(data=stream, **kwargs)

    def is_existing(self) -> bool:
        database = self.get_database()
        assert isinstance(database, ct.AbstractDatabase)
        return database.exists_table(self.get_path())

    def describe(self) -> Iterable:
        database = self.get_database()
        assert isinstance(database, ct.AbstractDatabase)
        return database.describe_table(self.get_path())

    def create(self, drop_if_exists: bool, verbose: AutoBool = arg.AUTO):
        database = self.get_database()
        assert isinstance(database, ct.AbstractDatabase)
        return database.create_table(
            self.get_name(),
            struct=self.struct,
            drop_if_exists=drop_if_exists,
            verbose=verbose,
        )

    def upload(
            self, data: Union[Iterable, Stream],
            encoding: Optional[str] = None, skip_first_line: bool = False,
            skip_lines: int = 0, max_error_rate: float = 0.0,
            verbose: AutoBool = arg.AUTO,
    ):
        database = self.get_database()
        assert isinstance(database, ct.AbstractDatabase)
        return database.safe_upload_table(
            self.get_name(),
            data=data,
            struct=self.get_struct(),
            skip_lines=skip_lines,
            skip_first_line=skip_first_line,
            encoding=encoding,
            max_error_rate=max_error_rate,
            verbose=verbose,
        )
