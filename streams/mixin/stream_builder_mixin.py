from abc import ABC
from typing import Iterable, Callable, Any

try:  # Assume we're a submodule in a package.
    from interfaces import Stream, StreamBuilderInterface, StreamType, ItemType, OptionalFields
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import Stream, StreamBuilderInterface, StreamType, ItemType, OptionalFields


class StreamBuilderMixin(StreamBuilderInterface, ABC):
    def stream(
            self,
            data: Iterable,
            item_type: ItemType = ItemType.Auto,
            ex: OptionalFields = None,
            **kwargs
    ) -> Stream:
        default_class = self.__class__
        if isinstance(item_type, StreamType):
            stream_class = item_type.get_class(default=default_class)
        else:
            stream_class = default_class
            kwargs['item_type'] = item_type
        return stream_class(data, **kwargs)

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
