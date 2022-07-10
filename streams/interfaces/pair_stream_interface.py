from abc import ABC, abstractmethod
from typing import Optional, Union, Iterable, Callable

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from streams.interfaces.abstract_stream_interface import StreamInterface
    from streams.interfaces.regular_stream_interface import RegularStreamInterface
    from streams.stream_type import StreamType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from .abstract_stream_interface import StreamInterface
    from .regular_stream_interface import RegularStreamInterface
    from ..stream_type import StreamType

Stream = StreamInterface
Native = StreamInterface
OptionalFields = Optional[Union[Iterable, str]]
OptStreamType = Union[StreamType, arg.DefaultArgument]


class PairStreamInterface(StreamInterface, ABC):
    @abstractmethod
    def values(self) -> Stream:
        pass

    @abstractmethod
    def keys(self, uniq: bool, stream_type: OptStreamType = arg.DEFAULT) -> Stream:
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
    def stream(self, data: Iterable, stream_type: OptStreamType = arg.DEFAULT, **kwargs) -> Stream:
        pass

    @abstractmethod
    def get_dict(self, of_lists: bool = True) -> dict:
        pass
