from abc import ABC, abstractmethod
from typing import Iterable, Callable

try:  # Assume we're a submodule in a package.
    from content.items.item_type import ItemType
    from streams.interfaces.abstract_stream_interface import StreamInterface, OptionalFields
    from streams.interfaces.regular_stream_interface import RegularStreamInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...content.items.item_type import ItemType
    from .abstract_stream_interface import StreamInterface, OptionalFields
    from .regular_stream_interface import RegularStreamInterface

Stream = StreamInterface
Native = StreamInterface


class PairStreamInterface(StreamInterface, ABC):
    @abstractmethod
    def values(self) -> Stream:
        pass

    @abstractmethod
    def keys(self, uniq: bool, stream_type: ItemType = ItemType.Auto) -> Stream:
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
    def stream(self, data: Iterable, stream_type: ItemType = ItemType.Auto, **kwargs) -> Stream:
        pass

    @abstractmethod
    def get_dict(self, of_lists: bool = True) -> dict:
        pass
