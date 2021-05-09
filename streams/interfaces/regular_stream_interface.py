from abc import ABC, abstractmethod
from typing import Optional, Union, Iterable, Callable

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from items.base_item_type import ItemType
    from streams.interfaces.abstract_stream_interface import StreamInterface
    from streams.stream_type import StreamType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...items.base_item_type import ItemType
    from .abstract_stream_interface import StreamInterface
    from ..stream_type import StreamType

Stream = StreamInterface
OptionalFields = Optional[Union[Iterable, str]]


class RegularStreamInterface(StreamInterface, ABC):
    @staticmethod
    def get_item_type() -> ItemType:
        return ItemType.Any

    @abstractmethod
    def is_empty(self) -> bool:
        pass

    @abstractmethod
    def map(self, func: Callable) -> Stream:
        pass

    @abstractmethod
    def map_to(self, function, stream_type) -> Stream:
        pass

    @abstractmethod
    def flat_map(self, function) -> Stream:
        pass

    @abstractmethod
    def filter(self, *functions) -> Stream:
        pass

    @abstractmethod
    def select(self, *columns, **expressions) -> Stream:
        pass

    @abstractmethod
    def apply_to_data(self, function, *args, save_count=False, lazy=True, stream_type=arg.DEFAULT, **kwargs) -> Stream:
        pass

    @abstractmethod
    def sorted_group_by(self, *keys, values: Optional[Iterable] = None, as_pairs: bool = False) -> Stream:
        pass

    @abstractmethod
    def group_by(self, *keys, values: Optional[Iterable] = None, as_pairs: bool = False) -> Stream:
        pass

    @abstractmethod
    def stream(self, data: Iterable, ex: OptionalFields = None, **kwargs) -> Stream:
        pass

    @abstractmethod
    def to_stream(
            self,
            data: Union[Iterable, arg.DefaultArgument] = arg.DEFAULT,
            stream_type=arg.DEFAULT,
            ex: OptionalFields = None,
            **kwargs
    ) -> Stream:
        pass

    @abstractmethod
    def get_columns(self) -> Optional[Iterable]:
        pass
