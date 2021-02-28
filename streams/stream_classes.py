from enum import Enum
import inspect
import gc

MAX_ITEMS_IN_MEMORY = 5000000
TMP_FILES_TEMPLATE = 'stream_{}.tmp'
TMP_FILES_ENCODING = 'utf8'

try:  # Assume we're a sub-module in a package.
    from streams.abstract.abstract_stream import AbstractStream
    from streams.abstract.iterable_stream import IterableStream
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
    from .abstract.abstract_stream import AbstractStream
    from .abstract.iterable_stream import IterableStream
    from .simple.any_stream import AnyStream
    from .simple.line_stream import LineStream
    from .simple.row_stream import RowStream
    from .pairs.key_value_stream import KeyValueStream
    from .typed.schema_stream import SchemaStream
    from .simple.record_stream import RecordStream
    from .typed.pandas_stream import PandasStream
    from ..utils import arguments as arg
    from ..schema import schema_classes as sh

STREAM_CLASSES = (
    AbstractStream, IterableStream,
    AnyStream,
    LineStream, RowStream, RecordStream,
    KeyValueStream,
    PandasStream, SchemaStream,
)
context = None


class StreamType(Enum):
    AnyStream = 'AnyStream'
    LineStream = 'LineStream'
    RowStream = 'RowStream'
    KeyValueStream = 'KeyValueStream'
    SchemaStream = 'SchemaStream'
    RecordStream = 'RecordStream'
    PandasStream = 'PandasStream'

    def get_class(self):
        classes = dict(
            AnyStream=AnyStream,
            LineStream=LineStream,
            RowStream=RowStream,
            KeyValueStream=KeyValueStream,
            SchemaStream=SchemaStream,
            RecordStream=RecordStream,
            PandasStream=PandasStream,
        )
        return classes.get(self.value)

    def stream(self, *args, **kwargs):
        stream_class = self.get_class()
        return stream_class(*args, **kwargs)


def get_class(stream_type):
    if inspect.isclass(stream_type):
        return stream_type
    if isinstance(stream_type, str):
        stream_type = StreamType(stream_type)
    message = 'stream_type must be an instance of StreamType (but {} as type {} received)'
    assert isinstance(stream_type, StreamType), TypeError(message.format(stream_type, type(stream_type)))
    return stream_type.get_class()


def get_context():
    global context
    return context


def set_context(cx):
    global context
    context = cx


def stream(stream_type, *args, **kwargs):
    if is_stream_class(STREAM_CLASSES):
        stream_class = stream_type
    else:
        stream_class = StreamType(stream_type).get_class()
    if 'context' not in kwargs:
        kwargs['context'] = get_context()
    return stream_class(*args, **kwargs)


def is_stream_class(obj):
    return obj in STREAM_CLASSES


def is_stream(obj):
    return isinstance(obj, STREAM_CLASSES)


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
