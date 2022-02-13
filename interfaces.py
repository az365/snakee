from typing import Optional, Callable, Union, Any

try:  # Assume we're a submodule in a package.
    from base.classes.typing import (
        ARRAY_TYPES, Array, Count, Columns, OptionalFields, Options, Message,
        FieldName, FieldNo, FieldID, Name, Value, Class, Links,
        AUTO, Auto, AutoName, AutoCount, AutoBool, AutoColumns, AutoLinks,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .base.classes.typing import (
        ARRAY_TYPES, Array, Count, Columns, OptionalFields, Options, Message,
        FieldName, FieldNo, FieldID, Name, Value, Class, Links,
        AUTO, Auto, AutoName, AutoCount, AutoBool, AutoColumns, AutoLinks,
    )

try:  # Assume we're a submodule in a package.
    from base.interfaces.base_interface import BaseInterface  # ROOT
    from base.interfaces.sourced_interface import SourcedInterface  # inherits Base[Interface]
    from base.interfaces.data_interface import SimpleDataInterface  # inherits Base[Interface]
    from base.interfaces.iterable_interface import IterableInterface  # inherits SimpleDataInterface
    from series.interfaces.any_series_interface import AnySeriesInterface  # inherits IterableInterface
    from series.interfaces.sorted_series_interface import SortedSeriesInterface  # inherits AnySeriesInterface
    from series.interfaces.numeric_series_interface import NumericSeriesInterface  # inherits AnySeriesInterface
    from series.interfaces.sorted_numeric_series_interface import SortedNumericSeriesInterface  # Sorted, Numeric
    from series.interfaces.date_series_interface import DateSeriesInterface  # inherits AnySeriesInterface
    from series.interfaces.key_value_series_interface import KeyValueSeriesInterface  # inherits DateSeriesInterface
    from series.interfaces.sorted_key_value_series_interface import SortedKeyValueSeriesInterface  # Sorted, KeyValue
    from series.interfaces.sorted_numeric_key_value_series_interface import SortedNumericKeyValueSeriesInterface
    from series.interfaces.date_numeric_series_interface import DateNumericSeriesInterface  # SortedNumericKeyValueSe..
    from streams.interfaces.abstract_stream_interface import StreamInterface  # inherits Sourced
    from streams.interfaces.iterable_stream_interface import IterableStreamInterface  # inherits Stream
    from streams.interfaces.local_stream_interface import LocalStreamInterface  # inherits IterableStream
    from streams.interfaces.regular_stream_interface import RegularStreamInterface  # inherits Stream
    from streams.interfaces.pair_stream_interface import PairStreamInterface  # inherits Regular, uses Stream
    from streams.interfaces.columnar_interface import ColumnarInterface  # inherits RegularStream
    from streams.interfaces.stream_builder_interface import StreamBuilderInterface  # inherits Stream
    from base.interfaces.context_interface import ContextInterface  # inherits Base; uses Stream, Logger, ExtendedLog..
    from base.interfaces.contextual_interface import ContextualInterface  # inherits Sourced; uses Base, Context
    from base.interfaces.data_interface import ContextualDataInterface  # inherits SimpleData, Contextual
    from base.interfaces.tree_interface import TreeInterface  # inherits ContextualData
    from content.representations.repr_interface import RepresentationInterface
    from content.fields.field_interface import FieldInterface  # inherits SimpleData
    from loggers.extended_logger_interface import LoggerInterface  # ROOT
    from loggers.extended_logger_interface import ExtendedLoggerInterface  # inherits Sourced, Logger; uses Base
    from loggers.selection_logger_interface import SelectionLoggerInterface  # inherits Extended, uses DetailedMessage
    from loggers.progress_interface import ProgressInterface  # inherits Tree; uses ExtendedLogger
    from connectors.interfaces.connector_interface import ConnectorInterface  # inherits Sourced, uses ExtendedLogger,..
    from connectors.interfaces.leaf_connector_interface import LeafConnectorInterface  # inherits Connector
    from connectors.interfaces.temporary_interface import TemporaryLocationInterface  # inherits Connector
    from connectors.interfaces.temporary_interface import TemporaryFilesMaskInterface  # inherits Connector
    from content.format.format_interface import ContentFormatInterface  # inherits Base
    from content.struct.struct_interface import StructInterface, StructMixinInterface  # ROOT
    from content.struct.struct_row_interface import StructRowInterface  # inherits SimpleData; uses StructInterface
    from content.items.simple_items import (
        ROW_SUBCLASSES, RECORD_SUBCLASSES, LINE_SUBCLASSES, STAR, Line,
        FrozenDict, SimpleRecord, MutableRecord, ImmutableRecord, Record,
        SimpleRowInterface, SimpleRow, MutableRow, ImmutableRow, Row,
        SimpleSelectableItem, SimpleItem, Item,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .base.interfaces.base_interface import BaseInterface  # ROOT
    from .base.interfaces.sourced_interface import SourcedInterface  # inherits Base[Interface]
    from .base.interfaces.data_interface import SimpleDataInterface  # inherits Base[Interface]
    from .base.interfaces.iterable_interface import IterableInterface  # inherits SimpleDataInterface
    from .series.interfaces.any_series_interface import AnySeriesInterface  # inherits IterableInterface
    from .series.interfaces.sorted_series_interface import SortedSeriesInterface  # inherits AnySeriesInterface
    from .series.interfaces.numeric_series_interface import NumericSeriesInterface  # inherits AnySeriesInterface
    from .series.interfaces.sorted_numeric_series_interface import SortedNumericSeriesInterface  # Sorted, Numeric
    from .series.interfaces.date_series_interface import DateSeriesInterface  # inherits AnySeriesInterface
    from .series.interfaces.key_value_series_interface import KeyValueSeriesInterface  # inherits DateSeriesInterface
    from .series.interfaces.sorted_key_value_series_interface import SortedKeyValueSeriesInterface  # Sorted, KeyValue
    from .series.interfaces.sorted_numeric_key_value_series_interface import SortedNumericKeyValueSeriesInterface
    from .series.interfaces.date_numeric_series_interface import DateNumericSeriesInterface  # SortedNumericKeyValueSe..
    from .streams.interfaces.abstract_stream_interface import StreamInterface  # inherits Sourced
    from .streams.interfaces.iterable_stream_interface import IterableStreamInterface  # inherits Stream
    from .streams.interfaces.local_stream_interface import LocalStreamInterface  # inherits IterableStream
    from .streams.interfaces.regular_stream_interface import RegularStreamInterface  # inherits Stream
    from .streams.interfaces.pair_stream_interface import PairStreamInterface  # inherits Regular, uses Stream
    from .streams.interfaces.columnar_interface import ColumnarInterface  # inherits RegularStream
    from .streams.interfaces.stream_builder_interface import StreamBuilderInterface  # inherits Stream
    from .base.interfaces.context_interface import ContextInterface  # inherits Base; uses Stream, Logger, ExtendedLog..
    from .base.interfaces.contextual_interface import ContextualInterface  # inherits Sourced; uses Base, Context
    from .base.interfaces.data_interface import ContextualDataInterface  # inherits SimpleData, Contextual
    from .base.interfaces.tree_interface import TreeInterface  # inherits ContextualData
    from .content.representations.repr_interface import RepresentationInterface
    from .content.fields.field_interface import FieldInterface  # inherits SimpleData
    from .loggers.extended_logger_interface import LoggerInterface  # ROOT
    from .loggers.extended_logger_interface import ExtendedLoggerInterface  # inherits Sourced, Logger; uses Base
    from .loggers.selection_logger_interface import SelectionLoggerInterface  # inherits Extended, uses DetailedMessage
    from .loggers.progress_interface import ProgressInterface  # inherits Tree; uses ExtendedLogger
    from .connectors.interfaces.connector_interface import ConnectorInterface  # inherits Sourced, uses ExtendedLogger,.
    from .connectors.interfaces.leaf_connector_interface import LeafConnectorInterface  # inherits Connector
    from .connectors.interfaces.temporary_interface import TemporaryLocationInterface  # inherits Connector
    from .connectors.interfaces.temporary_interface import TemporaryFilesMaskInterface  # inherits Connector
    from .content.format.format_interface import ContentFormatInterface  # inherits Base
    from .content.struct.struct_interface import StructInterface, StructMixinInterface  # ROOT
    from .content.struct.struct_row_interface import StructRowInterface  # inherits SimpleData; uses StructInterface
    from .content.items.simple_items import (
        ROW_SUBCLASSES, RECORD_SUBCLASSES, LINE_SUBCLASSES, STAR, Line,
        FrozenDict, SimpleRecord, MutableRecord, ImmutableRecord, Record,
        SimpleRowInterface, SimpleRow, MutableRow, ImmutableRow, Row,
        SimpleSelectableItem, SimpleItem, Item,
    )

try:  # Assume we're a submodule in a package.
    from utils.algo import JoinType  # standard Enum
    from loggers.extended_logger_interface import LoggingLevel  # standard Enum
    from loggers.progress_interface import OperationStatus  # standard Enum
    from connectors.databases.dialect_type import DialectType  # inherits DynamicEnum
    from connectors.filesystem.folder_type import FolderType  # inherits ClassType(DynamicEnum)
    from connectors.conn_type import ConnType  # inherits ClassType(DynamicEnum)
    from content.format.content_type import ContentType  # inherits ClassType(DynamicEnum)
    from content.representations.repr_type import ReprType  # inherits ClassType(DynamicEnum)
    from content.fields.field_type import FieldType  # inherits DynamicEnum
    from content.items.item_type import ItemType  # inherits SubclassesType(ClassType)
    from streams.stream_type import StreamType  # inherits ClassType(DynamicEnum)
    from series.series_type import SeriesType  # inherits ClassType(DynamicEnum)
    from series.interpolation_type import InterpolationType  # inherits DynamicEnum
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .utils.algo import JoinType  # standard Enum
    from .loggers.extended_logger_interface import LoggingLevel  # standard Enum
    from .loggers.progress_interface import OperationStatus  # standard Enum
    from .connectors.databases.dialect_type import DialectType  # inherits DynamicEnum
    from .connectors.filesystem.folder_type import FolderType  # inherits ClassType(DynamicEnum)
    from .connectors.conn_type import ConnType  # inherits ClassType(DynamicEnum)
    from .content.format.content_type import ContentType  # inherits ClassType(DynamicEnum)
    from .content.representations.repr_type import ReprType  # inherits ClassType(DynamicEnum)
    from .content.fields.field_type import FieldType  # inherits DynamicEnum
    from .content.items.item_type import ItemType  # inherits SubclassesType(ClassType)
    from .streams.stream_type import StreamType  # inherits ClassType(DynamicEnum)
    from .series.series_type import SeriesType  # inherits ClassType(DynamicEnum)
    from .series.interpolation_type import InterpolationType  # inherits DynamicEnum

Field = Union[FieldID, FieldInterface]
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

Source = Union[ConnectorInterface, Auto, None]
Connector = Optional[ConnectorInterface]
LeafConnector = LeafConnectorInterface
ExtLogger = ExtendedLoggerInterface
SelectionLogger = SelectionLoggerInterface
Context = Optional[ContextInterface]
TmpFiles = TemporaryFilesMaskInterface

AutoContext = Union[Auto, Context]
AutoConnector = Union[Auto, Connector]
AutoStreamType = Union[Auto, StreamType]
UniKey = Union[StructInterface, Array, AutoName, Callable]
