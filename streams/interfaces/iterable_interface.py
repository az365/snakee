from abc import ABC, abstractmethod
from typing import Optional, Callable, Iterable, Generator, Union, Any

try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from utils.algo import JoinType
    from base.interfaces.data_interface import SimpleDataInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...utils.algo import JoinType
    from ...base.interfaces.data_interface import SimpleDataInterface

Native = SimpleDataInterface
OptionalFields = Optional[Union[str, Iterable]]
Item = Any


class IterableInterface(SimpleDataInterface, ABC):
    @abstractmethod
    def is_in_memory(self) -> Optional[bool]:
        """Checks is the data of stream in RAM or in external iterator.

        :return: True if stream has data as Sequence in memory, False if it has an iterator
        """
        pass

    @abstractmethod
    def collect(self, inplace: bool = False, **kwargs) -> Native:
        pass

    @abstractmethod
    def get_count(self) -> Optional[int]:
        pass

    @abstractmethod
    def get_str_count(self, default: str = '(iter)') -> str:
        """Returns string with general information about expected or estimated count of items in stream.
        """
        pass

    @abstractmethod
    def make_new(self, *args, count: Optional[int] = None, ex: OptionalFields = None, **kwargs) -> Native:
        pass

    @abstractmethod
    def is_empty(self) -> Optional[bool]:
        pass

    @abstractmethod
    def get_items(self) -> Iterable:
        pass

    @abstractmethod
    def get_iter(self) -> Generator:
        """Returns Generator (Iterable) over stream data.

        :return: Generator
        """
        pass

    @abstractmethod
    def __iter__(self):
        """Returns link to Iterable data in stream.

        :return: list or Iterator
        """
        pass

    @abstractmethod
    def get_one_item(self) -> Item:
        pass

    @abstractmethod
    def take(self, count: Union[int, bool] = 1) -> Native:
        """Return stream containing first N items.
        Alias for head().

        :param count: count of items to return
        :type count: int

        :return: Native Stream (stream of same class)
        """
        pass

    @abstractmethod
    def skip(self, count: int = 1) -> Native:
        """Return stream with items except first N items.

        :param count: count of items to skip
        :type count: int

        :return: Native Stream (stream of same class)
        """
        pass

    @abstractmethod
    def head(self, count: int = 10) -> Native:
        """Return stream containing first N items.
        Alias for take()

        :param count: count of items to return
        :type count: int

        :return: Native Stream (stream of same class)
        """
        pass

    @abstractmethod
    def tail(self, count: int = 10) -> Native:
        """Return stream containing last N items from current stream.

        :param count: count of items to return
        :type count: int

        :return: Native Stream (stream of same class)
        """
        pass

    @abstractmethod
    def pass_items(self) -> Native:
        """Receive and skip all items from data source.
        Can be used for case when data source must be sure that all data has been transmitted successfully.

        :return: Native Stream (stream of same class)
        """
        pass

    @abstractmethod
    def copy(self) -> Native:
        pass

    @abstractmethod
    def add(self, obj_or_items: Union[Native, Iterable], before: bool = False, **kwargs) -> Native:
        pass

    @abstractmethod
    def add_items(self, items: Iterable, before: bool = False) -> Native:
        pass

    @abstractmethod
    def split_by_pos(self, pos: int) -> tuple:
        pass

    @abstractmethod
    def split_by_list_pos(self, list_pos: Union[list, tuple]) -> list:
        pass

    @abstractmethod
    def split_by_numeric(self, func: Callable, count: int) -> list:
        pass

    @abstractmethod
    def split_by_boolean(self, func: Callable) -> list:
        pass

    @abstractmethod
    def split(self, by: Union[int, list, tuple, Callable], count: Optional[int] = None) -> Iterable:
        pass

    @abstractmethod
    def split_to_iter_by_step(self, step: int) -> Iterable:
        pass

    @abstractmethod
    def filter(self, function: Callable) -> Native:
        pass

    @abstractmethod
    def map(self, function: Callable) -> Native:
        pass

    @abstractmethod
    def flat_map(self, function: Callable) -> Native:
        pass

    @abstractmethod
    def map_side_join(
            self,
            right: Native,
            key,
            how: Union[JoinType, str] = JoinType.Left,
            right_is_uniq: bool = True,
    ) -> Native:
        pass

    @abstractmethod
    def get_dict(self, key: Callable, value: Callable) -> dict:
        pass

    @abstractmethod
    def get_demo_example(self, count: int = 3) -> Iterable:
        pass

    @abstractmethod
    def show(self, *args, **kwargs):
        pass

    @abstractmethod
    def print(self, stream_function: Union[Callable, str] = 'get_count', *args, **kwargs) -> Native:
        pass

    @abstractmethod
    def submit(
            self,
            external_object: Union[list, dict, Callable] = print,
            stream_function: Union[Callable, str] = 'get_count',
            key: Optional[str] = None, show=False,
    ) -> Native:
        pass
