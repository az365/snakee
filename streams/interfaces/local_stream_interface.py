from abc import ABC, abstractmethod
from typing import Optional, Iterable, Callable, Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from streams.interfaces.iterable_stream_interface import IterableStreamInterface
    from connectors.interfaces.connector_interface import ConnectorInterface
    from connectors.filesystem.temporary_interface import TemporaryFilesMaskInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ..interfaces.iterable_stream_interface import IterableStreamInterface
    from connectors.interfaces.connector_interface import ConnectorInterface
    from ...connectors.filesystem.temporary_interface import TemporaryFilesMaskInterface

Native = IterableStreamInterface
Array = Union[list, tuple]
Key = Union[str, Array, Callable]
Count = Union[int, arg.Auto, None]
Verbose = Union[bool, arg.Auto]

AUTO = arg.AUTO


class LocalStreamInterface(IterableStreamInterface, ABC):
    @abstractmethod
    def get_limit_items_in_memory(self) -> int:
        pass

    @abstractmethod
    def set_limit_items_in_memory(self, count: Count, inplace: bool) -> Optional[Native]:
        pass

    @abstractmethod
    def limit_items_in_memory(self, count: Count) -> Native:
        pass

    @abstractmethod
    def get_list(self) -> list:
        pass

    @abstractmethod
    def to_iter(self) -> Native:
        pass

    @abstractmethod
    def can_be_in_memory(self, step: Count = AUTO) -> bool:
        pass

    @abstractmethod
    def to_memory(self) -> Native:
        pass

    @abstractmethod
    def collect(self, inplace: bool = True) -> Native:
        pass

    def memory_sort(self, key: Key = lambda a: a, reverse: bool = False, verbose: bool = False) -> Native:
        pass

    @abstractmethod
    def disk_sort(
            self,
            key: Key = lambda a: a,
            reverse: bool = False,
            step: Count = AUTO,
            verbose: bool = False,
    ) -> Native:
        pass

    @abstractmethod
    def sort(self, *keys, reverse: bool = False, step: Count = arg.AUTO, verbose: bool = True) -> Native:
        pass

    @abstractmethod
    def sorted_join(
            self,
            right: Native,
            key: Key,
            how: str = 'left',
            sorting_is_reversed: bool = False,
    ) -> Native:
        pass

    @abstractmethod
    def join(
            self,
            right: Native, key: Key, how='left',
            reverse: bool = False,
            is_sorted=False, right_is_uniq: bool = False,
            allow_map_side: bool = True, force_map_side: bool = True,
            verbose: Verbose = AUTO,
    ) -> Native:
        pass

    @abstractmethod
    def split_to_disk_by_step(
            self,
            step: Count = arg.AUTO,
            sort_each_by: Optional[str] = None,
            reverse: bool = False,
            verbose: bool = True,
    ) -> Iterable:
        pass

    @abstractmethod
    def is_empty(self) -> Optional[bool]:
        pass

    @abstractmethod
    def update_count(self, force: bool = False, skip_iter: bool = True) -> Native:
        pass

    @abstractmethod
    def get_tmp_files(self) -> TemporaryFilesMaskInterface:
        pass

    @abstractmethod
    def remove_tmp_files(self) -> int:
        pass

    @abstractmethod
    def get_encoding(self, default: str = 'utf8') -> str:
        pass

    @abstractmethod
    def get_mask(self) -> str:
        pass

    @abstractmethod
    def get_str_description(self) -> str:
        pass
