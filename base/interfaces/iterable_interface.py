from abc import ABC, abstractmethod
from typing import Optional, Callable, Iterable, Generator, Union, Any

try:  # Assume we're a submodule in a package.
    from utils.algo import JoinType
    from base.interfaces.data_interface import SimpleDataInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.algo import JoinType
    from .data_interface import SimpleDataInterface

Native = SimpleDataInterface
OptionalFields = Union[str, Iterable, None]
Item = Any


class IterableInterface(SimpleDataInterface, ABC):
    @abstractmethod
    def is_sequence(self) -> bool:
        pass

    @abstractmethod
    def is_iter(self) -> bool:
        pass

    @abstractmethod
    def is_in_memory(self) -> Optional[bool]:
        """Checks is the data of iterable object is in RAM or in external iterator.

        :return: True if object has data as Sequence in memory, False if it has an iterator
        """
        pass

    @abstractmethod
    def to_memory(self) -> Native:
        """Accumulate data from internal Iterator/Generator (probably from external stream) to list in memory.
        Alias of collect(inplace=False) method.

        :return: Native (object of same class)
        """
        pass

    @abstractmethod
    def collect(self, inplace: bool = False, **kwargs) -> Native:
        """Accumulate data from internal Iterator/Generator (probably from external stream) to list in memory.
        Alias of to_memory() method.

        :param inplace: apply transform inplace seems change current object, otherwise build new object (outplace)
        :type inplace: bool

        :return: Native (object of same class)
        """
        pass

    @abstractmethod
    def get_count(self) -> Optional[int]:
        """Returns factual count of items if current object has Sequence in memory,
        otherwise try to get expected count from meta-information.

        :return: string with one or two numbers (expected and/or estimated count of items)
        """
        pass

    @abstractmethod
    def get_str_count(self, default: str = '(iter)') -> str:
        """Returns string with general information about
        factual, expected or estimated (from meta-information) count of items in iterable object.

        :param default: default string to output for case when object is Iterator/Generator and count is not known.
        :type default: str

        :return: string with one or two numbers (expected and/or estimated count of items)
        """
        pass

    @abstractmethod
    def make_new(self, *args, count: Optional[int] = None, ex: OptionalFields = None, **kwargs) -> Native:
        """Build new object of same class with new iterable data.
        Set actual count into meta-information if this class supports count-property.
        Set additional meta-information (if kwargs-argument provided)
        or exclude some meta-fields (if ex-argument provided).

        New iterable must be given in first positional argument (items=*args[0]).

        :param count: count of items to return
        :type count: int

        :param ex: expected items count or None (if not known)
        :type ex: OptionalFields = Union[str, Iterable, None]

        :return: new Native (object of same class)
        """
        return super().make_new(*args, ex=ex, **kwargs)  # pass

    @abstractmethod
    def is_empty(self) -> Optional[bool]:
        """Checks is object having any items.

        :return: Ture of False if object is Sequence, None if object is Iterator/Generator and count is not known.
        """
        pass

    @abstractmethod
    def has_items(self) -> Optional[bool]:
        """Checks is object having any items.

        :return: Ture of False if object is Sequence, None if object is Iterator/Generator and count is not known.
        """
        pass

    @abstractmethod
    def set_items(self, items: Iterable, inplace: bool, count: Optional[int] = None) -> Optional[Native]:
        pass

    @abstractmethod
    def get_items(self) -> Iterable:
        pass

    @abstractmethod
    def get_list(self) -> list:
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
    def get_one_item(self) -> Optional[Item]:
        """Returns first available item from Iterable object.

        :return: Any (first) Item if available or None if Iterator is finished.
        """
        pass

    @abstractmethod
    def take(self, count: Union[int, bool] = 1, inplace: bool = False) -> Optional[Native]:
        """Return transformed iterable object (stream, series, ...) containing first N items.
        Alias for head().

        :param count: count of items to return
        :type count: int

        :param inplace: apply transform inplace seems change current object, otherwise build new object (outplace)
        :type inplace: bool

        :return: Native (object of same class) if inplace=True, or None if inplace=False
        """
        pass

    @abstractmethod
    def skip(self, count: int = 1, inplace: bool = False) -> Optional[Native]:
        """Return transformed iterable object (stream, series, ...) with items except first N items.

        :param count: count of items to skip
        :type count: int

        :param inplace: apply transform inplace seems change current object, otherwise build new object (outplace)
        :type inplace: bool

        :return: Native (object of same class) if inplace=True, or None if inplace=False
        """
        pass

    @abstractmethod
    def head(self, count: int = 10, inplace: bool = False) -> Optional[Native]:
        """Return transformed iterable object (stream, series, ...) containing first N items.
        Alias for take()

        :param count: count of items to return
        :type count: int

        :param inplace: apply transform inplace seems change current object, otherwise build new object (outplace)
        :type inplace: bool

        :return: Native (object of same class) if inplace=True, or None if inplace=False
        """
        pass

    @abstractmethod
    def tail(self, count: int = 10, inplace: bool = False) -> Optional[Native]:
        """Return transformed iterable object (stream, series, ...) containing last N items from current stream.

        :param count: count of items to return
        :type count: int

        :param inplace: apply transform inplace seems change current object, otherwise build new object (outplace)
        :type inplace: bool

        :return: Native (object of same class) if inplace=True, or None if inplace=False
        """
        pass

    @abstractmethod
    def pass_items(self) -> Native:
        """Receive and skip all items from data source.
        Can be used for case when data source must be sure that all data has been transmitted successfully.

        :return: Native (object of same class)
        """
        pass

    @abstractmethod
    def get_tee_clones(self, count: int = 2) -> list:
        pass

    @abstractmethod
    def copy(self) -> Native:
        pass

    @abstractmethod
    def add(
            self,
            obj_or_items: Union[Native, Iterable],
            before: bool = False,
            inplace: bool = False,
            **kwargs
    ) -> Native:
        pass

    @abstractmethod
    def add_items(self, items: Iterable, before: bool = False, inplace: bool = False) -> Optional[Native]:
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
    def filter(self, function: Callable, inplace: bool = False) -> Optional[Native]:
        pass

    @abstractmethod
    def map(self, function: Callable, inplace: bool = False) -> Optional[Native]:
        pass

    @abstractmethod
    def flat_map(self, function: Callable, inplace: bool = False) -> Optional[Native]:
        pass

    @abstractmethod
    def map_side_join(
            self,
            right: Native,
            key,
            how: Union[JoinType, str] = JoinType.Left,
            right_is_uniq: bool = True,
            inplace: bool = False,
    ) -> Optional[Native]:
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
    def print(self, stream_function: Union[Callable, str] = '__str__', *args, **kwargs) -> Native:
        pass

    @abstractmethod
    def submit(
            self,
            external_object: Union[list, dict, Callable] = print,
            stream_function: Union[Callable, str] = 'get_count',
            key: Optional[str] = None, show=False,
    ) -> Native:
        pass
