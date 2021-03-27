from enum import Enum
import inspect
import gc
from datetime import datetime
from random import randint

MAX_ITEMS_IN_MEMORY = 5000000
TMP_FILES_TEMPLATE = 'stream_{}.tmp'
TMP_FILES_ENCODING = 'utf8'

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from streams.interfaces.abstract_stream_interface import StreamInterface
    from streams.interfaces.pair_stream_interface import PairStreamInterface
    from streams.abstract.abstract_stream import AbstractStream
    from streams.abstract.iterable_stream import IterableStream
    from streams.abstract.local_stream import LocalStream
    from streams.abstract.wrapper_stream import WrapperStream
    from streams.mixin.columnar_mixin import ColumnarMixin
    from streams.mixin.convert_mixin import ConvertMixin
    from streams.simple.any_stream import AnyStream
    from streams.simple.line_stream import LineStream
    from streams.simple.row_stream import RowStream
    from streams.pairs.key_value_stream import KeyValueStream
    from streams.simple.schema_stream import SchemaStream
    from streams.simple.record_stream import RecordStream
    from streams.wrappers.pandas_stream import PandasStream
    from schema import schema_classes as sh
    from loggers.logger_classes import deprecated_with_alternative
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from .interfaces.abstract_stream_interface import StreamInterface
    from .interfaces.pair_stream_interface import PairStreamInterface
    from .abstract.abstract_stream import AbstractStream
    from .abstract.iterable_stream import IterableStream
    from .abstract.local_stream import LocalStream
    from .abstract.wrapper_stream import WrapperStream
    from .mixin.columnar_mixin import ColumnarMixin
    from .mixin.convert_mixin import ConvertMixin
    from .simple.any_stream import AnyStream
    from .simple.line_stream import LineStream
    from .simple.row_stream import RowStream
    from .pairs.key_value_stream import KeyValueStream
    from .simple.schema_stream import SchemaStream
    from .simple.record_stream import RecordStream
    from .wrappers.pandas_stream import PandasStream
    from ..schema import schema_classes as sh
    from ..loggers.logger_classes import deprecated_with_alternative

STREAM_CLASSES = (
    AbstractStream, IterableStream,
    AnyStream,
    LineStream, RowStream, RecordStream,
    KeyValueStream,
    PandasStream, SchemaStream,
)
DICT_STREAM_CLASSES = dict(
    AnyStream=AnyStream,
    LineStream=LineStream,
    RowStream=RowStream,
    KeyValueStream=KeyValueStream,
    SchemaStream=SchemaStream,
    RecordStream=RecordStream,
    PandasStream=PandasStream,
)
DICT_METHOD_SUFFIX = dict(
    AnyStream='any_stream',
    LineStream='line_stream',
    RowStream='row_stream',
    KeyValueStream='key_value_stream',
    SchemaStream='schema_stream',
    RecordStream='record_stream',
    PandasStream='pandas_stream',
)
context = None  # global


class StreamType(Enum):
    AnyStream = 'AnyStream'
    LineStream = 'LineStream'
    RowStream = 'RowStream'
    KeyValueStream = 'KeyValueStream'
    SchemaStream = 'SchemaStream'
    RecordStream = 'RecordStream'
    PandasStream = 'PandasStream'

    def get_name(self):
        return self.value

    def get_class(self, default=STREAM_CLASSES[0]):
        global DICT_STREAM_CLASSES
        return DICT_STREAM_CLASSES.get(self.get_name(), default)

    def get_method_suffix(self):
        global DICT_METHOD_SUFFIX
        return DICT_METHOD_SUFFIX.get(self.get_name())

    def stream(self, *args, provide_context=True, **kwargs):
        stream_class = self.get_class()
        if provide_context:
            global context
            if context and not kwargs.get('context'):
                kwargs['context'] = context
        return stream_class(*args, **kwargs)

    @staticmethod
    def detect(obj):
        if inspect.isclass(obj):
            name = obj.__name__
        else:
            name = obj.__class__.__name__
            if name == 'ItemType':
                if obj.value == 'SchemaRow':
                    return StreamType.SchemaStream
                else:
                    return StreamType('{}Stream'.format(name))
        return StreamType(name)

    @classmethod
    def of(cls, obj):
        if isinstance(obj, StreamType):
            return obj
        elif isinstance(obj, str):
            return StreamType(obj)
        else:
            return cls.detect(obj)

    def isinstance(self, sm: AbstractStream):
        return sm.get_stream_type() == self


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
    return RowStream.is_valid_item_type(item)


def is_record(item):
    return RecordStream.is_valid_item_type(item)


def is_schema_row(item):
    return isinstance(item, sh.SchemaRow)


@deprecated_with_alternative('AbstractStream.generate_name()')
def generate_name():
    cur_time = datetime.now().strftime('%y%m%d_%H%M%S')
    random = randint(0, 1000)
    cur_name = '{}_{:03}'.format(cur_time, random)
    return cur_name


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
