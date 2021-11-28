from abc import ABC, abstractmethod
from typing import Optional, Iterable, Callable, Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from utils.external import DataFrame
    from items.item_type import ItemType
    from streams.stream_type import StreamType
    from streams.interfaces.abstract_stream_interface import StreamInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...utils.external import DataFrame
    from ...items.item_type import ItemType
    from ..stream_type import StreamType
    from .abstract_stream_interface import StreamInterface

Stream = StreamInterface
OptionalFields = Optional[Union[Iterable, str]]


class RegularStreamInterface(StreamInterface, ABC):
    @staticmethod
    def get_item_type() -> ItemType:
        """Returns ItemType object, representing expected type of iterable data

        :return: ItemType object
        """
        return ItemType.Any

    @abstractmethod
    def is_empty(self) -> bool:
        pass

    @abstractmethod
    def map(self, func: Callable) -> Stream:
        pass

    @abstractmethod
    def map_to(self, function: Callable, stream_type: StreamType) -> Stream:
        pass

    @abstractmethod
    def flat_map(self, function: Callable) -> Stream:
        pass

    @abstractmethod
    def filter(self, *functions) -> Stream:
        pass

    @abstractmethod
    def select(self, *columns, **expressions) -> Stream:
        pass

    @abstractmethod
    def apply_to_data(
            self, function: Callable, *args,
            save_count: bool = False, lazy: bool = True,
            stream_type: StreamType = arg.AUTO,
            **kwargs
    ) -> Stream:
        pass

    @abstractmethod
    def sorted_group_by(self, *keys, values: Optional[Iterable] = None, as_pairs: bool = False) -> Stream:
        pass

    @abstractmethod
    def group_by(self, *keys, values: Optional[Iterable] = None, as_pairs: bool = False) -> Stream:
        pass

    @abstractmethod
    def to_stream(
            self,
            data: Union[Iterable, arg.Auto] = arg.AUTO,
            stream_type=arg.AUTO,
            ex: OptionalFields = None,
            **kwargs
    ) -> Stream:
        pass

    @abstractmethod
    def add_stream(self, stream: Stream) -> Stream:
        pass

    @abstractmethod
    def collect(self) -> Stream:
        pass

    @abstractmethod
    def get_dataframe(self, columns: Optional[Iterable] = None) -> DataFrame:
        pass
