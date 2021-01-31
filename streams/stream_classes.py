from enum import Enum
import gc


MAX_ITEMS_IN_MEMORY = 5000000
TMP_FILES_TEMPLATE = 'stream_{}.tmp'
TMP_FILES_ENCODING = 'utf8'


try:  # Assume we're a sub-module in a package.
    from streams.simple.any_stream import AnyStream
    from streams.simple.line_stream import LineStream
    from streams.simple.row_stream import RowStream
    from streams.pairs.key_value_stream import KeyValueStream
    from streams.typed.schema_stream import SchemaStream
    from streams.simple.record_stream import RecordStream
    from streams.typed.pandas_stream import PandasStream
    from utils import arguments as arg
    from schema import schema_classes as sh
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .simple.any_stream import AnyStream
    from .simple.line_stream import LineStream
    from .simple.row_stream import RowStream
    from .pairs.key_value_stream import KeyValueStream
    from .typed.schema_stream import SchemaStream
    from .simple.record_stream import RecordStream
    from .typed.pandas_stream import PandasStream
    from ..utils import arguments as arg
    from ..schema import schema_classes as sh


class StreamType(Enum):
    AnyStream = 'AnyStream'
    LineStream = 'LineStream'
    RowStream = 'RowStream'
    KeyValueStream = 'KeyValueStream'
    SchemaStream = 'SchemaStream'
    RecordStream = 'RecordStream'
    PandasStream = 'PandasStream'


def get_class(stream_type):
    if isinstance(stream_type, str):
        stream_type = StreamType(stream_type)
    assert isinstance(stream_type, StreamType), TypeError(
        'stream_type must be an instance of StreamType (but {} as type {} received)'.format(stream_type, type(stream_type))
    )
    if stream_type == StreamType.AnyStream:
        return AnyStream
    elif stream_type == StreamType.LineStream:
        return LineStream
    elif stream_type == StreamType.RowStream:
        return RowStream
    elif stream_type == StreamType.KeyValueStream:
        return KeyValueStream
    elif stream_type == StreamType.SchemaStream:
        return SchemaStream
    elif stream_type == StreamType.RecordStream:
        return RecordStream
    elif stream_type == StreamType.PandasStream:
        return PandasStream


def is_stream(obj):
    return isinstance(
        obj,
        (AnyStream, LineStream, RowStream, KeyValueStream, SchemaStream, RecordStream, PandasStream),
    )


def is_row(item):
    return RowStream.is_valid_item(item)


def is_record(item):
    return RecordStream.is_valid_item(item)


def is_schema_row(item):
    return isinstance(item, sh.SchemaRow)


def concat(*iter_streams):
    iter_streams = arg.update(iter_streams)
    result = None
    for cur_stream in iter_streams:
        if result is None:
            result = cur_stream
        else:
            result = result.add_stream(cur_stream)
        gc.collect()
    return result
