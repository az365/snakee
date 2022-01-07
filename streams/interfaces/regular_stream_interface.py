from abc import ABC, abstractmethod
from typing import Optional, Iterable, Callable, Union

try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from utils.external import DataFrame
    from items.item_type import ItemType
    from streams.stream_type import StreamType
    from streams.interfaces.iterable_stream_interface import IterableStreamInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...utils.external import DataFrame
    from ...items.item_type import ItemType
    from ..stream_type import StreamType
    from ..interfaces.iterable_stream_interface import IterableStreamInterface

Stream = IterableStreamInterface
OptionalFields = Optional[Union[Iterable, str]]


class RegularStreamInterface(IterableStreamInterface, ABC):
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
    def map_to(self, function: Callable, stream_type: StreamType) -> Stream:
        pass

    @abstractmethod
    def flat_map(self, function: Callable) -> Stream:
        pass

    @abstractmethod
    def select(self, *columns, **expressions) -> Stream:
        pass

    @abstractmethod
    def sorted_group_by(
            self,
            *keys,
            values: Optional[Iterable] = None,
            as_pairs: bool = False,
    ) -> Stream:
        pass

    @abstractmethod
    def group_by(
            self,
            *keys,
            values: Optional[Iterable] = None,
            as_pairs: bool = False,
            take_hash: bool = True,
            step: Union[int, arg.Auto] = arg.AUTO,
            verbose: bool = True,
    ) -> Stream:
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
