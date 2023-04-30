from abc import ABC, abstractmethod
from typing import Optional, Iterable, Callable, Union

try:  # Assume we're a submodule in a package.
    from base.classes.typing import Count, Array
    from base.constants.text import DEFAULT_ENCODING
    from utils.algo import JoinType
    from connectors.interfaces.connector_interface import ConnectorInterface
    from connectors.interfaces.temporary_interface import TemporaryFilesMaskInterface
    from streams.interfaces.iterable_stream_interface import IterableStreamInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.typing import Count, Array
    from ...base.constants.text import DEFAULT_ENCODING
    from ...utils.algo import JoinType
    from ...connectors.interfaces.connector_interface import ConnectorInterface
    from ...connectors.interfaces.temporary_interface import TemporaryFilesMaskInterface
    from .iterable_stream_interface import IterableStreamInterface

Native = IterableStreamInterface
Key = Union[str, Array, Callable]


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
    def can_be_in_memory(self, step: Count = None) -> bool:
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
            step: Count = None,
            verbose: bool = False,
    ) -> Native:
        pass

    @abstractmethod
    def sort(self, *keys, reverse: bool = False, step: Count = None, verbose: bool = True) -> Native:
        pass

    @abstractmethod
    def sorted_join(
            self,
            right: Native,
            key: Key,
            how: JoinType = JoinType.Left,
            sorting_is_reversed: bool = False,
    ) -> Native:
        pass

    @abstractmethod
    def join(
            self,
            right: Native,
            key: Key,
            how: JoinType = JoinType.Left,
            reverse: bool = False,
            is_sorted: bool = False,
            right_is_uniq: bool = False,
            allow_map_side: bool = True,
            force_map_side: bool = True,
            verbose: Optional[bool] = None,
    ) -> Native:
        pass

    @abstractmethod
    def split_to_disk_by_step(
            self,
            step: Count = None,
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
    def get_encoding(self, default: str = DEFAULT_ENCODING) -> str:
        pass

    @abstractmethod
    def get_mask(self) -> str:
        pass

    @abstractmethod
    def get_str_description(self) -> str:
        pass
