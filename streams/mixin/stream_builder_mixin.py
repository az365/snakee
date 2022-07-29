from abc import ABC
from typing import Union, Iterable, Callable, Any
from inspect import isclass

try:  # Assume we're a submodule in a package.
    from interfaces import Stream, StreamBuilderInterface, StreamType, ItemType, OptionalFields, Auto, AUTO
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import Stream, StreamBuilderInterface, StreamType, ItemType, OptionalFields, Auto, AUTO


class StreamBuilderMixin(StreamBuilderInterface, ABC):
    def stream(
            self,
            data: Iterable,
            stream_type: Union[StreamType, Stream, Auto] = AUTO,
            ex: OptionalFields = None,
            **kwargs
    ) -> Stream:
        stream_type = Auto.acquire(stream_type, self.get_stream_type())
        if isinstance(stream_type, str):
            stream_class = StreamType(stream_type).get_class()
        elif isclass(stream_type):
            stream_class = stream_type
        else:
            stream_class = stream_type.get_class()
        meta = self.get_compatible_meta(stream_class, ex=ex)
        meta.update(kwargs)
        return StreamType.of(stream_type).stream(data, **meta)

    @classmethod
    def empty(cls, **kwargs):
        try:  # Assume we're StreamBuilder
            stream_class = cls.get_default_stream_class()
        except AttributeError:  # Apparently we're Stream class
            stream_class = cls
        empty_data = list()
        return stream_class(empty_data, **kwargs)

    def _get_calc(self, function: Callable, *args, **kwargs) -> Any:
        return function(self.get_data(), *args, **kwargs)

    def map(self, function: Callable) -> Stream:
        items = map(function, self.get_items())
        return self.stream(items)

    def filter(self, function: Callable) -> Stream:
        items = filter(function, self.get_items())
        return self.stream(items, count=None)

    def get_demo_example(self, count=3) -> Iterable:
        stream = self.take(count)
        assert isinstance(stream, Stream)
        yield from stream.get_items()
        source = self.get_source()
        if hasattr(source, 'close'):
            source.close()
