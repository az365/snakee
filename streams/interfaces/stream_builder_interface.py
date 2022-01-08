from abc import ABC, abstractmethod
from typing import Iterable, Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from content.items.item_type import ItemType
    from streams.stream_type import StreamType
    from streams.interfaces.abstract_stream_interface import StreamInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...content.items.item_type import ItemType
    from ..stream_type import StreamType
    from .abstract_stream_interface import StreamInterface

AutoStreamType = Union[StreamType, arg.Auto]
OptionalFields = Union[Iterable, str, None]


class StreamBuilderInterface(ABC):
    @abstractmethod
    def stream(
            self,
            data: Iterable,
            stream_type: AutoStreamType = arg.AUTO,
            ex: OptionalFields = None,
            **kwargs
    ) -> StreamInterface:
        pass
