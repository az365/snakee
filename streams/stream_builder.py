from typing import Union, Iterable, Any, Optional
import gc

try:  # Assume we're a sub-module in a package.
    from utils import (
        arguments as arg,
        numeric as nm,
        algo,
    )
    from items.base_item_type import ItemType
    from streams import stream_classes as sm
    from streams.interfaces.abstract_stream_interface import StreamInterface
    from functions import item_functions as fs
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import (
        arguments as arg,
        numeric as nm,
        algo,
    )
    from ..items.base_item_type import ItemType
    from . import stream_classes as sm
    from .interfaces.abstract_stream_interface import StreamInterface
    from ..functions import item_functions as fs

Stream = Union[StreamInterface, Any]
OptionalStreamType = Union[Any, arg.DefaultArgument]
OptionalFields = Optional[Union[Iterable, str]]


class StreamBuilder:
    _dict_classes = dict()
    _stream_types = None

    @classmethod
    def stream(
            cls,
            data: Iterable,
            stream_type: Union[OptionalStreamType, StreamInterface, arg.DefaultArgument] = arg.DEFAULT,
            **kwargs
    ) -> Stream:
        if not arg.is_defined(stream_type):
            example_item = cls._get_one_item(data)
            item_type = cls._detect_item_type(example_item)
            stream_type = cls._get_dict_classes('stream(stream_type=AUTO)').get(item_type)
        return cls._get_stream_types().of(stream_type).stream(data, **kwargs)

    @classmethod
    def stack(cls, *iter_streams, how='vertical', name=arg.DEFAULT, context=None, **kwargs):
        iter_streams = arg.update(iter_streams)
        result = None
        for cur_stream in iter_streams:
            assert isinstance(cur_stream, StreamInterface)
            if result is None:
                if hasattr(cur_stream, 'copy'):
                    result = cur_stream.copy()
                else:
                    result = cur_stream
                if arg.is_defined(name):
                    result.set_name(name)
                if arg.is_defined(context):
                    result.set_context(context)
            elif how == 'vertical':
                result = result.add_stream(cur_stream)
            else:
                result = result.join(cur_stream, how=how, **kwargs)
            gc.collect()
        return result

    @classmethod
    def concat(cls, *iter_streams, name=arg.DEFAULT, context=None):
        return cls.stack(*iter_streams, name=name, context=context)

    @classmethod
    def join(cls, *iter_streams, key, how='left', step=arg.DEFAULT, name=arg.DEFAULT, context=None):
        return cls.stack(*iter_streams, key=key, how=how, step=step, name=name, context=context)

    @classmethod
    def _get_dict_classes(cls, operation_name='this'):
        dict_classes = cls._dict_classes
        assert dict_classes, 'For {} operation dict_classes must be defined'.format(operation_name)
        return dict_classes

    @classmethod
    def _get_stream_types(cls, operation_name='this'):
        stream_types = cls._stream_types
        assert stream_types, 'For {} operation stream_types must be defined'.format(operation_name)
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
        elif hasattr(item, 'get_schema'):
            return ItemType.SchemaRow
        else:
            return ItemType.Any
