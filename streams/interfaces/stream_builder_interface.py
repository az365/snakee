from abc import ABC, abstractmethod
from typing import Iterable, Union

try:  # Assume we're a submodule in a package.
    from content.items.item_type import ItemType, Auto, AUTO
    from streams.stream_type import StreamType
    from streams.interfaces.abstract_stream_interface import StreamInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...content.items.item_type import ItemType, Auto, AUTO
    from ..stream_type import StreamType
    from .abstract_stream_interface import StreamInterface


class StreamBuilderInterface(ABC):
    @abstractmethod
    def stream(
            self,
            data: Iterable,
            stream_type: Union[StreamType, ItemType, Auto] = AUTO,
            ex: Union[Iterable, str, None] = None,
            **kwargs
    ) -> StreamInterface:
        pass

    @classmethod
    @abstractmethod
    def empty(cls, **kwargs) -> StreamInterface:
        pass
