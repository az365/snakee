from typing import Optional, Callable, Iterable, Iterator, Generator, Union, Any, NoReturn

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.interfaces.base_interface import BaseInterface  # ROOT
    from base.interfaces.sourced_interface import SourcedInterface  # inherits Base[Interface]
    from streams.interfaces.abstract_stream_interface import StreamInterface  # inherits Sourced
    from streams.interfaces.iterable_stream_interface import IterableStreamInterface  # inherits Stream
    from streams.interfaces.local_stream_interface import LocalStreamInterface  # inherits IterableStream
    from streams.interfaces.regular_stream_interface import RegularStreamInterface  # inherits Stream
    from streams.interfaces.pair_stream_interface import PairStreamInterface  # inherits Regular, uses Stream
    from base.interfaces.context_interface import ContextInterface  # inherits Base; uses Stream, Logger, ExtendedLog..
    from base.interfaces.contextual_interface import ContextualInterface  # inherits Sourced; uses Base, Context
    from base.interfaces.data_interface import SimpleDataInterface  # inherits BaseInterface
    from base.interfaces.data_interface import ContextualDataInterface  # inherits SimpleData, Contextual
    from base.interfaces.tree_interface import TreeInterface  # inherits ContextualData
    from fields.field_interface import FieldInterface  # inherits SimpleData
    from items.struct_interface import StructInterface  # ROOT
    from items.struct_row_interface import StructRowInterface  # inherits SimpleData; uses StructInterface
    from loggers.extended_logger_interface import LoggerInterface  # ROOT
    from loggers.extended_logger_interface import ExtendedLoggerInterface  # inherits Sourced, Logger; uses Base
    from loggers.selection_logger_interface import SelectionLoggerInterface  # inherits Extended, uses DetailedMessage
    from loggers.progress_interface import ProgressInterface  # inherits Tree; uses ExtendedLogger
    from connectors.abstract.connector_interface import ConnectorInterface  # inherits Sourced, uses ExtendedLogger, P..
    from connectors.abstract.connector_interface import LeafConnectorInterface  # inherits Connector
    from connectors.filesystem.temporary_interface import TemporaryLocationInterface  # inherits Connector
    from connectors.filesystem.temporary_interface import TemporaryFilesMaskInterface  # inherits Connector
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .utils import arguments as arg
    from .base.interfaces.base_interface import BaseInterface  # ROOT
    from .base.interfaces.sourced_interface import SourcedInterface  # inherits Base[Interface]
    from .streams.interfaces.abstract_stream_interface import StreamInterface  # inherits Sourced
    from .streams.interfaces.iterable_stream_interface import IterableStreamInterface  # inherits Stream
    from .streams.interfaces.local_stream_interface import LocalStreamInterface  # inherits IterableStream
    from .streams.interfaces.regular_stream_interface import RegularStreamInterface  # inherits Stream
    from .streams.interfaces.pair_stream_interface import PairStreamInterface  # inherits Regular, uses Stream
    from .base.interfaces.context_interface import ContextInterface  # inherits Base; uses Stream, Logger, ExtendedLog..
    from .base.interfaces.contextual_interface import ContextualInterface  # inherits Sourced; uses Base, Context
    from .base.interfaces.data_interface import SimpleDataInterface  # inherits BaseInterface
    from .base.interfaces.data_interface import ContextualDataInterface  # inherits SimpleData, Contextual
    from .base.interfaces.tree_interface import TreeInterface  # inherits ContextualData
    from .fields.field_interface import FieldInterface  # inherits SimpleData
    from .items.struct_interface import StructInterface  # ROOT
    from .items.struct_row_interface import StructRowInterface  # inherits SimpleData; uses StructInterface
    from .loggers.extended_logger_interface import LoggerInterface  # ROOT
    from .loggers.extended_logger_interface import ExtendedLoggerInterface  # inherits Sourced, Logger; uses Base
    from .loggers.selection_logger_interface import SelectionLoggerInterface  # inherits Extended, uses DetailedMessage
    from .loggers.progress_interface import ProgressInterface  # inherits Tree; uses ExtendedLogger
    from .connectors.abstract.connector_interface import ConnectorInterface  # inherits Sourced, uses ExtendedLogger, P.
    from .connectors.abstract.connector_interface import LeafConnectorInterface  # inherits Connector
    from .connectors.filesystem.temporary_interface import TemporaryLocationInterface  # inherits Connector
    from .connectors.filesystem.temporary_interface import TemporaryFilesMaskInterface  # inherits Connector

try:  # Assume we're a sub-module in a package.
    from loggers.extended_logger_interface import LoggingLevel  # standard Enum
    from loggers.progress_interface import OperationStatus  # standard Enum
    from streams.stream_type import StreamType  # inherits ClassType(DynamicEnum)
    from fields.field_type import FieldType  # inherits DynamicEnum
    from items.item_type import ItemType  # inherits SubclassesType(ClassType)
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .loggers.extended_logger_interface import LoggingLevel  # standard Enum
    from .loggers.progress_interface import OperationStatus  # standard Enum
    from .streams.stream_type import StreamType  # inherits ClassType(DynamicEnum)
    from .fields.field_type import FieldType  # inherits DynamicEnum
    from .items.item_type import ItemType  # inherits SubclassesType(ClassType)

Auto = arg.Auto
Name = Union[str, int]
Count = Optional[int]
Array = Union[list, tuple]
Message = Union[str, Array]
Field = Union[Name, FieldInterface]
OptionalFields = Union[Array, str, None]
Columns = Optional[Array]
AutoColumns = Union[Auto, Columns]


Line = str
Record = dict
Row = Array
StructRow = StructRowInterface
RegularItem = Union[Line, Record, Row, StructRow]
Item = Union[Any, RegularItem]

Stream = StreamInterface
IterableStream = IterableStreamInterface
LocalStream = LocalStreamInterface
RegularStream = RegularStreamInterface
LineStream = RegularStream
RowStream = RegularStream
RecordStream = RegularStream
KeyValueStream = RegularStream
StructStream = RegularStream
StructStream = StructStream

Source = Union[ConnectorInterface, arg.Auto, None]
Connector = Optional[ConnectorInterface]
LeafConnector = LeafConnectorInterface
ExtLogger = ExtendedLoggerInterface
SelectionLogger = SelectionLoggerInterface
Context = Optional[ContextInterface]
TmpFiles = TemporaryFilesMaskInterface

AutoName = Union[Auto, Name]
AutoCount = Union[Auto, Count]
AutoBool = Union[arg.Auto, bool]
AutoContext = Union[Auto, Context]
AutoConnector = Union[arg.Auto, Connector]
UniKey = Union[StructInterface, Array, AutoName, Callable]

AUTO = arg.AUTO
ARRAY_TYPES = list, tuple
