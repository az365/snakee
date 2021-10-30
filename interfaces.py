from typing import Optional, Callable, Union, Any

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.interfaces.base_interface import BaseInterface  # ROOT
    from base.interfaces.sourced_interface import SourcedInterface  # inherits Base[Interface]
    from streams.interfaces.abstract_stream_interface import StreamInterface  # inherits Sourced
    from streams.interfaces.iterable_stream_interface import IterableStreamInterface  # inherits Stream
    from streams.interfaces.local_stream_interface import LocalStreamInterface  # inherits IterableStream
    from streams.interfaces.regular_stream_interface import RegularStreamInterface  # inherits Stream
    from streams.interfaces.pair_stream_interface import PairStreamInterface  # inherits Regular, uses Stream
    from streams.interfaces.columnar_interface import ColumnarInterface  # inherits RegularStream
    from streams.interfaces.stream_builder_interface import StreamBuilderInterface  # inherits Stream
    from base.interfaces.context_interface import ContextInterface  # inherits Base; uses Stream, Logger, ExtendedLog..
    from base.interfaces.contextual_interface import ContextualInterface  # inherits Sourced; uses Base, Context
    from base.interfaces.data_interface import SimpleDataInterface  # inherits BaseInterface
    from base.interfaces.data_interface import ContextualDataInterface  # inherits SimpleData, Contextual
    from base.interfaces.tree_interface import TreeInterface  # inherits ContextualData
    from fields.field_interface import FieldInterface  # inherits SimpleData
    from loggers.extended_logger_interface import LoggerInterface  # ROOT
    from loggers.extended_logger_interface import ExtendedLoggerInterface  # inherits Sourced, Logger; uses Base
    from loggers.selection_logger_interface import SelectionLoggerInterface  # inherits Extended, uses DetailedMessage
    from loggers.progress_interface import ProgressInterface  # inherits Tree; uses ExtendedLogger
    from connectors.interfaces.connector_interface import ConnectorInterface  # inherits Sourced, uses ExtendedLogger,..
    from connectors.interfaces.leaf_connector_interface import LeafConnectorInterface  # inherits Connector
    from connectors.interfaces.struct_file_interface import StructFileInterface  # inherits LeafConnectorInterface
    from connectors.interfaces.temporary_interface import TemporaryLocationInterface  # inherits Connector
    from connectors.interfaces.temporary_interface import TemporaryFilesMaskInterface  # inherits Connector
    from items.struct_interface import StructInterface  # ROOT
    from items.struct_row_interface import StructRowInterface  # inherits SimpleData; uses StructInterface
    from items.simple_items import (
        ARRAY_TYPES, ROW_SUBCLASSES, RECORD_SUBCLASSES,
        SimpleRowInterface, SimpleRow, Row, Record, Line, SimpleItem, SimpleSelectableItem,
        FieldNo, FieldName, FieldID, Value, Array,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .utils import arguments as arg
    from .base.interfaces.base_interface import BaseInterface  # ROOT
    from .base.interfaces.sourced_interface import SourcedInterface  # inherits Base[Interface]
    from .streams.interfaces.abstract_stream_interface import StreamInterface  # inherits Sourced
    from .streams.interfaces.iterable_stream_interface import IterableStreamInterface  # inherits Stream
    from .streams.interfaces.local_stream_interface import LocalStreamInterface  # inherits IterableStream
    from .streams.interfaces.regular_stream_interface import RegularStreamInterface  # inherits Stream
    from .streams.interfaces.pair_stream_interface import PairStreamInterface  # inherits Regular, uses Stream
    from .streams.interfaces.columnar_interface import ColumnarInterface  # inherits RegularStream
    from .streams.interfaces.stream_builder_interface import StreamBuilderInterface  # inherits Stream
    from .base.interfaces.context_interface import ContextInterface  # inherits Base; uses Stream, Logger, ExtendedLog..
    from .base.interfaces.contextual_interface import ContextualInterface  # inherits Sourced; uses Base, Context
    from .base.interfaces.data_interface import SimpleDataInterface  # inherits BaseInterface
    from .base.interfaces.data_interface import ContextualDataInterface  # inherits SimpleData, Contextual
    from .base.interfaces.tree_interface import TreeInterface  # inherits ContextualData
    from .fields.field_interface import FieldInterface  # inherits SimpleData
    from .loggers.extended_logger_interface import LoggerInterface  # ROOT
    from .loggers.extended_logger_interface import ExtendedLoggerInterface  # inherits Sourced, Logger; uses Base
    from .loggers.selection_logger_interface import SelectionLoggerInterface  # inherits Extended, uses DetailedMessage
    from .loggers.progress_interface import ProgressInterface  # inherits Tree; uses ExtendedLogger
    from .connectors.interfaces.connector_interface import ConnectorInterface  # inherits Sourced, uses ExtendedLogger,.
    from .connectors.interfaces.leaf_connector_interface import LeafConnectorInterface  # inherits Connector
    from .connectors.interfaces.struct_file_interface import StructFileInterface  # inherits LeafConnectorInterface
    from .connectors.interfaces.temporary_interface import TemporaryLocationInterface  # inherits Connector
    from .connectors.interfaces.temporary_interface import TemporaryFilesMaskInterface  # inherits Connector
    from .items.struct_interface import StructInterface  # ROOT
    from .items.struct_row_interface import StructRowInterface  # inherits SimpleData; uses StructInterface
    from .items.simple_items import (
        ARRAY_TYPES, ROW_SUBCLASSES, RECORD_SUBCLASSES,
        SimpleRowInterface, SimpleRow, Row, Record, Line, SimpleItem, SimpleSelectableItem,
        FieldNo, FieldName, FieldID, Value, Array,
    )

try:  # Assume we're a sub-module in a package.
    from utils.algo import JoinType  # standard Enum
    from loggers.extended_logger_interface import LoggingLevel  # standard Enum
    from loggers.progress_interface import OperationStatus  # standard Enum
    from connectors.databases.dialect_type import DialectType  # inherits DynamicEnum
    from connectors.filesystem.folder_type import FolderType  # inherits ClassType(DynamicEnum)
    from connectors.filesystem.file_type import FileType  # inherits ClassType(DynamicEnum)
    from connectors.conn_type import ConnType  # inherits ClassType(DynamicEnum)
    from streams.stream_type import StreamType  # inherits ClassType(DynamicEnum)
    from fields.field_type import FieldType  # inherits DynamicEnum
    from items.item_type import ItemType  # inherits SubclassesType(ClassType)
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .utils.algo import JoinType  # standard Enum
    from .loggers.extended_logger_interface import LoggingLevel  # standard Enum
    from .loggers.progress_interface import OperationStatus  # standard Enum
    from .connectors.databases.dialect_type import DialectType  # inherits DynamicEnum
    from .connectors.filesystem.folder_type import FolderType  # inherits ClassType(DynamicEnum)
    from .connectors.filesystem.file_type import FileType  # inherits ClassType(DynamicEnum)
    from .connectors.conn_type import ConnType  # inherits ClassType(DynamicEnum)
    from .streams.stream_type import StreamType  # inherits ClassType(DynamicEnum)
    from .fields.field_type import FieldType  # inherits DynamicEnum
    from .items.item_type import ItemType  # inherits SubclassesType(ClassType)

AUTO = arg.AUTO
Auto = arg.Auto
Name = FieldID
Count = Optional[int]
Message = Union[str, Array]
Columns = Optional[Array]
Field = Union[FieldID, FieldInterface]
OptionalFields = Union[Array, str, None]
Options = Union[dict, arg.Auto, None]
How = Union[JoinType, str]

StructRow = StructRowInterface
RegularItem = Union[SimpleItem, StructRow]
Item = Union[Any, RegularItem]

Stream = StreamInterface
IterableStream = IterableStreamInterface
LocalStream = LocalStreamInterface
RegularStream = RegularStreamInterface
ColumnarStream = Union[RegularStream, ColumnarInterface]
LineStream = RegularStream
RowStream = ColumnarStream
RecordStream = ColumnarStream
KeyValueStream = ColumnarStream
StructStream = ColumnarStream
SchemaStream = StructStream

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
AutoStreamType = Union[arg.Auto, StreamType]
AutoColumns = Union[Auto, Columns]
UniKey = Union[StructInterface, Array, AutoName, Callable]
