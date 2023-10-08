from abc import ABC, abstractmethod
from typing import Iterable

try:  # Assume we're a submodule in a package.
    from content.items.item_type import ItemType
    from streams.interfaces.abstract_stream_interface import StreamInterface
    from streams.interfaces.regular_stream_interface import RegularStreamInterface, OptionalFields
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...content.items.item_type import ItemType
    from .abstract_stream_interface import StreamInterface
    from .regular_stream_interface import RegularStreamInterface, OptionalFields


class StreamBuilderInterface(ABC):
    @abstractmethod
    def stream(
            self,
            data: Iterable,
            item_type: ItemType = ItemType.Auto,
            ex: OptionalFields = None,
            **kwargs
    ) -> StreamInterface:
        pass

    @classmethod
    @abstractmethod
    def empty(cls, **kwargs) -> StreamInterface:
        pass
