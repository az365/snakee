from abc import ABC, abstractmethod
from inspect import isclass
from typing import Iterable, Callable, Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from interfaces import (
        IterableStreamInterface,
        ItemType, StreamType,
        Auto, AUTO, AutoName, OptionalFields,
    )
    from streams.mixin.iterable_mixin import IterableStreamMixin
    from streams.mixin.stream_builder_mixin import StreamBuilderMixin
    from connectors.abstract.leaf_connector import LeafConnector
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...interfaces import (
        IterableStreamInterface,
        ItemType, StreamType,
        Auto, AUTO, AutoName, OptionalFields,
    )
    from ...streams.mixin.iterable_mixin import IterableStreamMixin
    from ...streams.mixin.stream_builder_mixin import StreamBuilderMixin
    from ..abstract.leaf_connector import LeafConnector

Stream = IterableStreamInterface
Native = Union[LeafConnector, Stream]


class StreamFileMixin(IterableStreamMixin, ABC):
    @abstractmethod
    def is_existing(self) -> bool:
        pass

    @abstractmethod
    def is_in_memory(self) -> bool:
        pass

    @staticmethod
    def get_default_item_type() -> ItemType:
        return ItemType.Any

    @classmethod
    def get_stream_type(cls):
        return StreamType.AnyStream

    @classmethod
    def get_stream_class(cls):
        return cls.get_stream_type().get_class()

    def _get_generated_stream_name(self) -> str:
        return arg.get_generated_name('{}:stream'.format(self.get_name()), include_random=True, include_datetime=False)

    @abstractmethod
    def from_stream(self, stream: Stream, verbose: bool = True) -> Native:
        pass

    def to_stream(
            self,
            data: Union[Iterable, Auto] = AUTO, name: AutoName = AUTO,
            stream_type: Union[StreamType, Auto] = AUTO, ex: OptionalFields = None,
            **kwargs
    ) -> Stream:
        stream_type = arg.delayed_acquire(stream_type, self.get_stream_type)
        name = arg.delayed_acquire(name, self._get_generated_stream_name)
        if not arg.is_defined(data):
            data = self.get_items()
        if isinstance(stream_type, str):
            stream_class = StreamType(stream_type).get_class()
        elif isclass(stream_type):
            stream_class = stream_type
        else:
            stream_class = stream_type.get_class()
        meta = self.get_compatible_meta(stream_class, name=name, ex=ex, **kwargs)
        if 'count' not in meta:
            meta['count'] = self.get_count()
        if 'source' not in meta:
            meta['source'] = self
        return stream_class(data, **meta)

    def stream(
            self, data: Union[Iterable, Auto] = AUTO,
            stream_type: Union[StreamType, Auto] = AUTO,
            ex: OptionalFields = None,
            **kwargs
    ) -> Stream:
        return self.to_stream(data, stream_type=stream_type, ex=ex, **kwargs)

    def map(self, function: Callable) -> Stream:
        return self.stream(
            map(function, self.get_items()),
        )

    def filter(self, function: Callable) -> Stream:
        return self.stream(
            filter(function, self.get_items()),
            count=None,
        )

    def add_stream(self, stream: Stream, **kwargs) -> Stream:
        stream = self.to_stream(**kwargs).add_stream(stream)
        return self._assume_stream(stream)

    def collect(self, skip_missing: bool = False, **kwargs) -> Stream:
        if self.is_existing():
            stream = self.to_stream(**kwargs)
            if hasattr(stream, 'collect'):
                stream = stream.collect()
            elif not skip_missing:
                raise TypeError('stream {} of type {} can not be collected'.format(stream, stream.get_stream_type()))
        elif skip_missing:
            stream = self.get_stream_class()([])
        else:
            raise FileNotFoundError('File {} not found'.format(self.get_name()))
        return self._assume_stream(stream)

    @staticmethod
    def _assume_stream(obj) -> Stream:
        return obj

    def get_children(self) -> dict:  # ?
        return self._data
