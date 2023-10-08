from typing import Optional, Iterable, Iterator, Generator
import gc
from itertools import chain

try:  # Assume we're a submodule in a package.
    from interfaces import (
        StreamBuilderInterface,
        StreamInterface, LocalStreamInterface, ContextInterface, ConnectorInterface, TemporaryLocationInterface,
        StreamType, ItemType, JoinType,
        Stream, How, Class, OptionalFields,
    )
    from base.functions.arguments import update
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..interfaces import (
        StreamBuilderInterface,
        StreamInterface, LocalStreamInterface, ContextInterface, ConnectorInterface, TemporaryLocationInterface,
        StreamType, ItemType, JoinType,
        Stream, How, Class, OptionalFields,
    )
    from ..base.functions.arguments import update

Native = StreamBuilderInterface


class StreamBuilder(StreamBuilderInterface):
    _default_stream_class = None  # will be substituted in stream_classes.py
    _dict_classes = dict()
    _stream_types = StreamType
    _context = None

    @classmethod
    def stream(
            cls,
            data: Iterable,
            item_type: ItemType = ItemType.Auto,
            register: bool = True,
            **kwargs
    ) -> Stream:
        default_class = cls.get_default_stream_class()
        if isinstance(item_type, StreamType):
            stream_class = item_type.get_class(default=default_class)
        else:
            stream_class = default_class
            if 'item_type' not in kwargs:
                if isinstance(item_type, ItemType):
                    pass
                elif item_type not in (ItemType.Auto, None):
                    try:
                        item_type = ItemType(item_type)
                    except (TypeError, ValueError):
                        item_type = StreamType(item_type).get_item_type()
                else:
                    example_item = cls._get_one_item(data)
                    item_type = cls._detect_item_type(example_item)
                    if isinstance(data, (Iterator, Generator)):
                        data = chain([example_item], data)
                kwargs['item_type'] = item_type
        return stream_class(data, **kwargs)

    @classmethod
    def empty(cls, register: bool = False, **kwargs) -> StreamInterface:
        empty_data = list()
        return cls.stream(empty_data, register=register, **kwargs)

    @staticmethod
    def is_same_item_type(*iter_streams) -> bool:
        iter_streams = update(iter_streams)
        item_types = [i.get_item_type() for i in iter_streams]
        return len(set(item_types)) == 1

    @classmethod
    def stack(cls, *iter_streams, how: How = 'vertical', name: Optional[str] = None, context=None, **kwargs):
        iter_streams = update(iter_streams)
        assert cls.is_same_item_type(iter_streams), f'concat(): streams must have same type: {iter_streams}'
        result = None
        for cur_stream in iter_streams:
            assert isinstance(cur_stream, StreamInterface)
            if result is None:
                if hasattr(cur_stream, 'copy'):
                    result = cur_stream.copy()
                else:
                    result = cur_stream
                if name is not None:
                    result.set_name(name)
                if context is not None:
                    result.set_context(context)
            elif how == 'vertical':
                result = result.add_stream(cur_stream)
            else:
                result = result.join(cur_stream, how=how, **kwargs)
            gc.collect()
        return result

    @classmethod
    def concat(cls, *iter_streams, name: Optional[str] = None, context=None):
        return cls.stack(*iter_streams, name=name, context=context)

    @classmethod
    def join(cls, *iter_streams, key, how: How = JoinType.Left, step: Optional[int] = None, name=None, context=None):
        return cls.stack(*iter_streams, key=key, how=how, step=step, name=name, context=context)

    @classmethod
    def _get_dict_classes(cls, operation_name='this'):
        dict_classes = cls._dict_classes
        assert dict_classes, f'For {operation_name} operation dict_classes must be defined'
        return dict_classes

    @classmethod
    def _get_stream_types(cls, operation_name='this'):
        stream_types = cls._stream_types
        assert stream_types, f'For {operation_name} operation stream_types must be defined'
        return stream_types

    @staticmethod
    def _get_one_item(data: Iterable):
        for i in data:
            return i

    @staticmethod
    def _detect_item_type(item) -> ItemType:
        if isinstance(item, str):
            return ItemType.Line
        elif isinstance(item, dict):
            return ItemType.Record
        elif isinstance(item, (list, tuple)):
            return ItemType.Row
        else:
            return ItemType.Any

    @classmethod
    def get_default_stream_class(cls) -> Class:
        stream_class = cls._default_stream_class
        assert stream_class, 'StreamBuilder.get_default_stream_class(): _default_stream_class member not initialized'
        return stream_class

    @classmethod
    def set_default_stream_class(cls, stream_class: Class) -> None:
        cls._default_stream_class = stream_class

    @classmethod
    def get_context(cls) -> Optional[ContextInterface]:
        return cls._context

    @classmethod
    def set_context(cls, cx: ContextInterface, set_storage: bool = False) -> Native:
        cls._context = cx
        if set_storage:
            storage = cx.get_local_storage()
            if storage is not None:
                assert isinstance(storage, TemporaryLocationInterface)
                cls.set_temporary_location(storage)
        return cls()

    def _set_context_inplace(self, cx: ContextInterface) -> None:
        self.set_context(cx)

    context = property(get_context, _set_context_inplace)

    @classmethod
    def set_temporary_location(cls, storage: TemporaryLocationInterface) -> Native:
        default_stream_class = cls.get_default_stream_class()
        if default_stream_class is not None:
            if hasattr(default_stream_class, 'get_tmp_files'):  # isinstance(default_stream_class, LocalStream):
                temporary_location = default_stream_class.get_tmp_files()
                assert isinstance(storage, ConnectorInterface)
                temporary_location.set_default_storage(storage)
        return cls()
