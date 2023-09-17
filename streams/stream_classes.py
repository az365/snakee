from typing import Optional
from datetime import datetime
from random import randint

try:  # Assume we're a submodule in a package.
    from utils.decorators import deprecated_with_alternative
    from interfaces import (
        StreamInterface, IterableStreamInterface, LocalStreamInterface, RegularStreamInterface, PairStreamInterface,
        StreamBuilderInterface, ContextInterface, ConnectorInterface,
        TemporaryLocationInterface, TemporaryFilesMaskInterface,
        StreamType, ItemType, JoinType, How,
    )
    from base.constants.text import DEFAULT_ENCODING
    from base.functions.errors import get_type_err_msg
    from streams.abstract.abstract_stream import AbstractStream
    from streams.abstract.iterable_stream import IterableStream, MAX_ITEMS_IN_MEMORY
    from streams.abstract.local_stream import LocalStream
    from streams.abstract.wrapper_stream import WrapperStream
    from streams.mixin.columnar_mixin import ColumnarMixin
    from streams.mixin.convert_mixin import ConvertMixin
    from streams.regular.regular_stream import RegularStream
    from streams.regular.line_stream import LineStream
    from streams.regular.row_stream import RowStream
    from streams.pairs.key_value_stream import KeyValueStream
    from streams.regular.record_stream import RecordStream
    from streams.wrappers.pandas_stream import PandasStream
    from streams.wrappers.sql_stream import SqlStream
    from streams.stream_builder import StreamBuilder
    from connectors.filesystem.temporary_files import TemporaryLocation
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils.decorators import deprecated_with_alternative
    from ..interfaces import (
        StreamInterface, IterableStreamInterface, LocalStreamInterface, RegularStreamInterface, PairStreamInterface,
        StreamBuilderInterface, ContextInterface, ConnectorInterface,
        TemporaryLocationInterface, TemporaryFilesMaskInterface,
        StreamType, ItemType, JoinType, How,
    )
    from ..base.constants.text import DEFAULT_ENCODING
    from ..base.functions.errors import get_type_err_msg
    from .abstract.abstract_stream import AbstractStream
    from .abstract.iterable_stream import IterableStream, MAX_ITEMS_IN_MEMORY
    from .abstract.local_stream import LocalStream
    from .abstract.wrapper_stream import WrapperStream
    from .mixin.columnar_mixin import ColumnarMixin
    from .mixin.convert_mixin import ConvertMixin
    from .regular.regular_stream import RegularStream
    from .regular.line_stream import LineStream
    from .regular.row_stream import RowStream
    from .pairs.key_value_stream import KeyValueStream
    from .regular.record_stream import RecordStream
    from .wrappers.pandas_stream import PandasStream
    from .wrappers.sql_stream import SqlStream
    from .stream_builder import StreamBuilder
    from ..connectors.filesystem.temporary_files import TemporaryLocation

TMP_FILES_TEMPLATE = 'stream_{}.tmp'

DEFAULT_STREAM_CLASS = RegularStream
STREAM_CLASSES = (
    AbstractStream, IterableStream,
    RegularStream,
    LineStream, RowStream, RecordStream,
    KeyValueStream,
    PandasStream, SqlStream,
)
DICT_STREAM_CLASSES = dict(
    RegularStream=RegularStream,
    LineStream=LineStream,
    RowStream=RowStream,
    KeyValueStream=KeyValueStream,
    RecordStream=RecordStream,
    PandasStream=PandasStream,
    SqlStream=SqlStream,
)

# global
_context = None  # deprecated, use StreamBuilder.context instead


StreamType.set_default(RegularStream.__name__)
StreamType.set_dict_classes(DICT_STREAM_CLASSES)


@deprecated_with_alternative('StreamType.get_class()')
def get_class(stream_type):
    return StreamType(stream_type).get_class()


DICT_ITEM_TO_STREAM_TYPE = {
    ItemType.Any: StreamType.RegularStream,
    ItemType.Line: StreamType.LineStream,
    ItemType.Record: StreamType.RecordStream,
    ItemType.Row: StreamType.RowStream,
}
StreamBuilder._dict_classes = DICT_ITEM_TO_STREAM_TYPE
StreamBuilder.set_default_stream_class(DEFAULT_STREAM_CLASS)


@deprecated_with_alternative('StreamBuilder.get_context()')
def get_context() -> Optional[ContextInterface]:
    return StreamBuilder.get_context()


@deprecated_with_alternative('StreamBuilder.set_context()')
def set_context(cx: ContextInterface) -> None:
    global _context
    _context = cx
    storage = cx.get_local_storage()
    assert isinstance(storage, ConnectorInterface)
    TemporaryLocation.set_default_storage(storage)


@deprecated_with_alternative('StreamBuilder.stream()')
def stream(item_type: Optional[StreamType], *args, **kwargs) -> StreamInterface:
    if is_stream_class(item_type):
        stream_class = item_type
    elif item_type not in (ItemType.Auto, None):
        stream_class = StreamType(item_type).get_class()
    else:
        stream_class = DEFAULT_STREAM_CLASS
    if isinstance(item_type, ItemType):
        kwargs['item_type'] = item_type
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
    assert isinstance(location, TemporaryLocation), get_type_err_msg(location, TemporaryLocation, 'get_tmp_folder()')
    tmp_mask = location.mask(name)
    assert isinstance(tmp_mask, TemporaryFilesMaskInterface)
    return tmp_mask


@deprecated_with_alternative('StreamBuilder.concat()')
def concat(*iter_streams, context=None) -> StreamInterface:
    global _context
    if context is None:
        context = _context
    return StreamBuilder.concat(*iter_streams, context=context)


@deprecated_with_alternative('StreamBuilder.join()')
def join(*iter_streams, key, how: How = JoinType.Left, step=None, name=None, context=None) -> StreamInterface:
    global _context
    if context is None:
        context = _context
    return StreamBuilder.join(*iter_streams, key=key, how=how, step=step, name=name, context=context)
