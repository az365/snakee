from abc import ABC
from typing import Union, Iterable, Callable, Any
from inspect import isclass

try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from interfaces import Stream, StreamBuilderInterface, StreamType, ItemType, OptionalFields
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...interfaces import Stream, StreamBuilderInterface, StreamType, ItemType, OptionalFields


class StreamBuilderMixin(StreamBuilderInterface, ABC):
    def stream(
            self,
            data: Iterable,
            stream_type: Union[StreamType, Stream, arg.Auto] = arg.AUTO,
            ex: OptionalFields = None,
            **kwargs
    ) -> Stream:
        stream_type = arg.acquire(stream_type, self.get_stream_type())
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
        empty_data = list()
        return cls(empty_data, **kwargs)

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
