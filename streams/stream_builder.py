from typing import Union, Iterable
import gc

try:  # Assume we're a submodule in a package.
    from interfaces import (
        StreamInterface, Stream, StreamBuilderInterface,
        StreamType, ItemType, StreamItemType, JoinType, How, Class, OptionalFields, Auto, AUTO,
    )
    from base.functions.arguments import update
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..interfaces import (
        StreamInterface, Stream, StreamBuilderInterface,
        StreamType, ItemType, StreamItemType, JoinType, How, Class, OptionalFields, Auto, AUTO,
    )
    from ..base.functions.arguments import update


class StreamBuilder(StreamBuilderInterface):
    _default_stream_class = None  # will be substituted in stream_classes.py
    _dict_classes = dict()
    _stream_types = StreamType

    @classmethod
    def stream(
            cls,
            data: Iterable,
            stream_type: StreamItemType = AUTO,
            **kwargs
    ) -> Stream:
        default_class = cls.get_default_stream_class()
        if Auto.is_defined(stream_type) and isinstance(stream_type, StreamType):
            stream_class = stream_type.get_class(default=default_class)
        else:
            stream_class = default_class
            if 'item_type' not in kwargs:
                if isinstance(stream_type, ItemType):
                    item_type = stream_type
                elif Auto.is_defined(stream_type):
                    msg = f'StreamBuilder.stream(): expected stream_type as StreamType or ItemType, got {stream_type}'
                    raise TypeError(msg)
                else:
                    example_item = cls._get_one_item(data)
                    item_type = cls._detect_item_type(example_item)
                kwargs['item_type'] = item_type
        return stream_class(data, **kwargs)

    @classmethod
    def empty(cls, **kwargs) -> StreamInterface:
        pass

    @staticmethod
    def is_same_stream_type(*iter_streams) -> bool:
        iter_streams = update(iter_streams)
        stream_types = [i.get_stream_type() for i in iter_streams]
        return len(set(stream_types)) == 1

    @classmethod
    def stack(cls, *iter_streams, how: How = 'vertical', name=AUTO, context=None, **kwargs):
        iter_streams = update(iter_streams)
        assert cls.is_same_stream_type(iter_streams), 'concat(): streams must have same type: {}'.format(iter_streams)
        result = None
        for cur_stream in iter_streams:
            assert isinstance(cur_stream, StreamInterface)
            if result is None:
                if hasattr(cur_stream, 'copy'):
                    result = cur_stream.copy()
                else:
                    result = cur_stream
                if Auto.is_defined(name):
                    result.set_name(name)
                if Auto.is_defined(context):
                    result.set_context(context)
            elif how == 'vertical':
                result = result.add_stream(cur_stream)
            else:
                result = result.join(cur_stream, how=how, **kwargs)
            gc.collect()
        return result

    @classmethod
    def concat(cls, *iter_streams, name=AUTO, context=None):
        return cls.stack(*iter_streams, name=name, context=context)

    @classmethod
    def join(cls, *iter_streams, key, how: How = JoinType.Left, step=AUTO, name=AUTO, context=None):
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
        elif hasattr(item, 'get_struct'):
            return ItemType.StructRow
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
