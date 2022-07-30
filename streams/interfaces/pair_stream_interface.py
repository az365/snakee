from abc import ABC, abstractmethod
from typing import Iterable, Callable, Union

try:  # Assume we're a submodule in a package.
    from base.classes.auto import AUTO, Auto
    from content.items.item_type import ItemType
    from streams.stream_type import StreamType
    from streams.interfaces.abstract_stream_interface import StreamInterface
    from streams.interfaces.regular_stream_interface import RegularStreamInterface, StreamItemType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.auto import AUTO, Auto
    from ...content.items.item_type import ItemType
    from ..stream_type import StreamType
    from .abstract_stream_interface import StreamInterface
    from .regular_stream_interface import RegularStreamInterface, StreamItemType

Stream = StreamInterface
Native = StreamInterface
OptionalFields = Union[Iterable, str, None]


class PairStreamInterface(StreamInterface, ABC):
    @abstractmethod
    def values(self) -> Stream:
        pass

    @abstractmethod
    def keys(self, uniq: bool, stream_type: StreamItemType = AUTO) -> Stream:
        pass

    @abstractmethod
    def map_keys(self, func: Callable) -> Native:
        pass

    @abstractmethod
    def map_values(self, func: Callable) -> Native:
        pass

    @abstractmethod
    def ungroup_values(self) -> Native:
        pass

    @abstractmethod
    def stream(self, data: Iterable, stream_type: StreamItemType = AUTO, **kwargs) -> Stream:
        pass

    @abstractmethod
    def get_dict(self, of_lists: bool = True) -> dict:
        pass
