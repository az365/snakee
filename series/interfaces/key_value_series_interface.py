from abc import ABC, abstractmethod
from typing import Optional, Callable, Iterable, Generator, Union, Any

try:  # Assume we're a submodule in a package.
    from functions.primary import numeric as nm
    from series.interfaces.any_series_interface import AnySeriesInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...functions.primary import numeric as nm
    from .any_series_interface import AnySeriesInterface

Native = AnySeriesInterface
SortedSeries = AnySeriesInterface

if nm.np:  # numpy installed
    Mutable = Union[list, nm.np.ndarray]
    MUTABLE = list, nm.np.ndarray
else:
    Mutable = list
    MUTABLE = list


class KeyValueSeriesInterface(AnySeriesInterface, ABC):
    @abstractmethod
    def get_errors(self) -> Generator:
        pass

    @abstractmethod
    def has_valid_counts(self) -> bool:
        pass

    @classmethod
    @abstractmethod
    def from_items(cls, items: Iterable) -> Native:
        pass

    @classmethod
    @abstractmethod
    def from_dict(cls, my_dict: dict) -> Native:
        pass

    @abstractmethod
    def key_series(self, set_closure: bool = False) -> Native:
        pass

    @abstractmethod
    def value_series(self, set_closure: bool = False, name: Optional[str] = None) -> Native:
        pass

    @abstractmethod
    def get_value_by_key(self, key: Any, default: Any = None):
        pass

    @abstractmethod
    def get_keys(self) -> list:
        pass

    @abstractmethod
    def set_keys(self, keys: Iterable, inplace: bool, set_closure: bool = False) -> Optional[Native]:
        pass

    @abstractmethod
    def set_values(self, values: Iterable, inplace: bool, set_closure: bool = False, validate: bool = False) -> Native:
        pass

    @abstractmethod
    def get_items(self) -> Iterable:
        pass

    @abstractmethod
    def set_items(self, items: Iterable, inplace: bool, **kwargs) -> Optional[Native]:
        pass

    @abstractmethod
    def get_arg_min(self) -> Any:
        pass

    @abstractmethod
    def get_arg_max(self) -> Any:
        pass

    @abstractmethod
    def append(self, item: Any, inplace: bool) -> Optional[Native]:
        pass

    @abstractmethod
    def append_pair(self, key: Any, value: Any, inplace: bool) -> Optional[Native]:
        pass

    @abstractmethod
    def add(self, key_value_series: Native, before: bool = False, inplace: bool = False, **kwargs) -> Optional[Native]:
        pass

    @abstractmethod
    def filter_pairs(self, function: Callable, inplace: bool = False) -> Optional[Native]:
        pass

    @abstractmethod
    def filter_keys(self, function: Callable, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def filter_values(self, function: Callable, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def filter_values_defined(self, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def filter_keys_defined(self, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def filter_keys_between(self, key_min, key_max, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def map_keys(self, function: Callable, sorting_changed: bool = False, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def assume_date_numeric(self) -> Native:
        pass

    @abstractmethod
    def assume_sorted(self) -> SortedSeries:
        pass

    @abstractmethod
    def sort_by_keys(self, reverse: bool = False, inplace: bool = False) -> Optional[Native]:
        pass

    @abstractmethod
    def group_by_keys(self) -> Native:
        pass

    @abstractmethod
    def sum_by_keys(self) -> Native:
        pass

    @abstractmethod
    def mean_by_keys(self):
        pass

    @abstractmethod
    def plot(self, fmt: str = '-') -> None:
        pass
