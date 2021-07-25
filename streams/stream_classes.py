from typing import Optional, NoReturn
from datetime import datetime
from random import randint

MAX_ITEMS_IN_MEMORY = 5000000
TMP_FILES_TEMPLATE = 'stream_{}.tmp'
TMP_FILES_ENCODING = 'utf8'

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from items.base_item_type import ItemType
    from streams.stream_type import StreamType
    from streams.interfaces.abstract_stream_interface import StreamInterface
    from streams.interfaces.iterable_stream_interface import IterableStreamInterface
    from streams.interfaces.local_stream_interface import LocalStreamInterface
    from streams.interfaces.regular_stream_interface import RegularStreamInterface
    from streams.interfaces.pair_stream_interface import PairStreamInterface
    from streams.abstract.abstract_stream import AbstractStream
    from streams.abstract.iterable_stream import IterableStream
    from streams.abstract.local_stream import LocalStream
    from streams.abstract.wrapper_stream import WrapperStream
    from streams.mixin.columnar_mixin import ColumnarMixin
    from streams.mixin.convert_mixin import ConvertMixin
    from streams.regular.any_stream import AnyStream
    from streams.regular.line_stream import LineStream
    from streams.regular.row_stream import RowStream
    from streams.pairs.key_value_stream import KeyValueStream
    from streams.regular.schema_stream import SchemaStream
    from streams.regular.record_stream import RecordStream
    from streams.wrappers.pandas_stream import PandasStream
    from streams.wrappers.sql_stream import SqlStream
    from streams.stream_builder import StreamBuilder
    from connectors.filesystem.temporary_interface import TemporaryLocationInterface, TemporaryFilesMaskInterface
    from connectors.filesystem.temporary_files import TemporaryLocation
    from base.interfaces.context_interface import ContextInterface
    from items import legacy_classes as sh
    from utils.decorators import deprecated_with_alternative
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..items.base_item_type import ItemType
    from .stream_type import StreamType
    from .interfaces.abstract_stream_interface import StreamInterface
    from .interfaces.iterable_stream_interface import IterableStreamInterface
    from .interfaces.local_stream_interface import LocalStreamInterface
    from .interfaces.regular_stream_interface import RegularStreamInterface
    from .interfaces.pair_stream_interface import PairStreamInterface
    from .abstract.abstract_stream import AbstractStream
    from .abstract.iterable_stream import IterableStream
    from .abstract.local_stream import LocalStream
    from .abstract.wrapper_stream import WrapperStream
    from .mixin.columnar_mixin import ColumnarMixin
    from .mixin.convert_mixin import ConvertMixin
    from .regular.any_stream import AnyStream
    from .regular.line_stream import LineStream
    from .regular.row_stream import RowStream
    from .pairs.key_value_stream import KeyValueStream
    from .regular.schema_stream import SchemaStream
    from .regular.record_stream import RecordStream
    from .wrappers.pandas_stream import PandasStream
    from .wrappers.sql_stream import SqlStream
    from .stream_builder import StreamBuilder
    from ..connectors.filesystem.temporary_interface import TemporaryLocationInterface, TemporaryFilesMaskInterface
    from ..connectors.filesystem.temporary_files import TemporaryLocation
    from ..base.interfaces.context_interface import ContextInterface
    from ..schema import legacy_classes as sh
    from ..utils.decorators import deprecated_with_alternative

STREAM_CLASSES = (
    AbstractStream, IterableStream,
    AnyStream,
    LineStream, RowStream, RecordStream,
    SchemaStream,
    KeyValueStream,
    PandasStream, SqlStream,
)
DICT_STREAM_CLASSES = dict(
    AnyStream=AnyStream,
    LineStream=LineStream,
    RowStream=RowStream,
    KeyValueStream=KeyValueStream,
    SchemaStream=SchemaStream,
    RecordStream=RecordStream,
    PandasStream=PandasStream,
    SqlStream=SqlStream,
)

_context = None  # global


StreamType.set_default(AnyStream.__name__)
StreamType.set_dict_classes(DICT_STREAM_CLASSES)


@deprecated_with_alternative('StreamType.get_class()')
def get_class(stream_type):
    return StreamType(stream_type).get_class()


DICT_ITEM_TO_STREAM_TYPE = {
    ItemType.Any: StreamType.AnyStream,
    ItemType.Line: StreamType.LineStream,
    ItemType.Record: StreamType.RecordStream,
    ItemType.Row: StreamType.RowStream,
    ItemType.SchemaRow: StreamType.SchemaStream,
}
StreamBuilder._dict_classes = DICT_ITEM_TO_STREAM_TYPE


def get_context() -> Optional[ContextInterface]:
    global _context
    return _context


def set_context(cx: ContextInterface) -> NoReturn:
    global _context
    _context = cx


def stream(stream_type, *args, **kwargs) -> StreamInterface:
    if is_stream_class(STREAM_CLASSES):
        stream_class = stream_type
    else:
        stream_class = StreamType(stream_type).get_class()
    if 'context' not in kwargs:
        kwargs['context'] = get_context()
    return stream_class(*args, **kwargs)


def is_stream_class(obj) -> bool:
    return obj in STREAM_CLASSES


def is_stream(obj) -> bool:
    return isinstance(obj, STREAM_CLASSES)


def is_row(item) -> bool:
    return RowStream.is_valid_item_type(item)


def is_record(item) -> bool:
    return RecordStream.is_valid_item_type(item)


def is_schema_row(item) -> bool:
    return isinstance(item, sh.SchemaRow)


@deprecated_with_alternative('AbstractStream.generate_name()')
def generate_name() -> str:
    cur_time = datetime.now().strftime('%y%m%d_%H%M%S')
    random = randint(0, 1000)
    cur_name = '{}_{:03}'.format(cur_time, random)
    return cur_name


def get_tmp_mask(name: str) -> TemporaryFilesMaskInterface:
    context = get_context()
    if context:
        location = context.get_tmp_folder()
    else:
        location = TemporaryLocation()
    assert isinstance(location, TemporaryLocation), 'got {}'.format(type(location))
    return location.mask(name)


def concat(*iter_streams, context=arg.AUTO) -> StreamInterface:
    global _context
    context = arg.acquire(context, _context)
    return StreamBuilder.concat(*iter_streams, context=context)


def join(*iter_streams, key, how='left', step=arg.AUTO, name=arg.AUTO, context=None) -> StreamInterface:
    global _context
    context = arg.acquire(context, _context)
    return StreamBuilder.join(*iter_streams, key=key, how=how, step=step, name=name, context=context)
