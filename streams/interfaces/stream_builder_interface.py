from abc import ABC, abstractmethod
from typing import Iterable, Union

try:  # Assume we're a submodule in a package.
    from streams.stream_type import StreamType, Auto, AUTO
    from streams.interfaces.abstract_stream_interface import StreamInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..stream_type import StreamType, Auto, AUTO
    from .abstract_stream_interface import StreamInterface

AutoStreamType = Union[StreamType, Auto]
OptionalFields = Union[Iterable, str, None]


class StreamBuilderInterface(ABC):
    @abstractmethod
    def stream(
            self,
            data: Iterable,
            stream_type: AutoStreamType = AUTO,
            ex: OptionalFields = None,
            **kwargs
    ) -> StreamInterface:
        pass

    @classmethod
    @abstractmethod
    def empty(cls, **kwargs) -> StreamInterface:
        pass
