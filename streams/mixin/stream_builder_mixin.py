from abc import ABC, abstractmethod
from typing import Union, Iterable, Callable, Any, Optional
from inspect import isclass

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
    from ...utils import (
        arguments as arg,
        numeric as nm,
        algo,
    )
    from ...items.base_item_type import ItemType
    from .. import stream_classes as sm
    from ..interfaces.abstract_stream_interface import StreamInterface
    from ...functions import item_functions as fs

Stream = Union[StreamInterface, Any]
OptionalStreamType = Union[Any, arg.DefaultArgument]
OptionalFields = Optional[Union[Iterable, str]]


class StreamBuilderInterface(StreamInterface, ABC):
    @staticmethod
    @abstractmethod
    def get_stream_type() -> OptionalStreamType:
        pass

    @abstractmethod
    def stream(
            self,
            data: Iterable,
            stream_type: OptionalStreamType = arg.DEFAULT,
            ex: OptionalFields = None,
            **kwargs
    ) -> Stream:
        pass

    @abstractmethod
    def get_compatible_meta(self, stream_class, ex: OptionalFields = None) -> dict:
        pass


class StreamBuilderMixin(StreamBuilderInterface, ABC):
    def stream(
            self,
            data: Iterable,
            stream_type: Union[OptionalStreamType, StreamInterface, arg.DefaultArgument] = arg.DEFAULT,
            ex: OptionalFields = None,
            **kwargs
    ) -> Stream:
        stream_type = arg.undefault(stream_type, self.get_stream_type())
        if isinstance(stream_type, str):
            stream_class = sm.StreamType(stream_type).get_class()
        elif isclass(stream_type):
            stream_class = stream_type
        else:
            stream_class = stream_type.get_class()
        meta = self.get_compatible_meta(stream_class, ex=ex)
        meta.update(kwargs)
        return sm.StreamType.of(stream_type).stream(data, **meta)

    def get_calc(self, function: Callable, *args, **kwargs) -> Any:
        return function(self.get_data(), *args, **kwargs)

    def map(self, function: Callable) -> Stream:
        return self.stream(
            map(function, self.get_items()),
        )

    def filter(self, function: Callable) -> Stream:
        return self.stream(
            filter(function, self.get_items()),
            count=None,
        )

    def get_demo_example(self, count=3) -> Iterable:
        yield from self.take(count).get_items()
        source = self.get_source()
        if hasattr(source, 'close'):
            source.close()
