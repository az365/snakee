from abc import ABC, abstractmethod
from typing import Callable, Union, Any

try:  # Assume we're a submodule in a package.
    from base.classes.typing import Name
    from series.series_type import SeriesType
    from series.interfaces.any_series_interface import AnySeriesInterface
    from series.interfaces.date_series_interface import DateSeriesInterface
    from series.interfaces.numeric_series_interface import NumericSeriesInterface
    from series.interfaces.sorted_series_interface import SortedSeriesInterface
    from series.interfaces.key_value_series_interface import KeyValueSeriesInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.typing import Name
    from ..series_type import SeriesType
    from .any_series_interface import AnySeriesInterface
    from .date_series_interface import DateSeriesInterface
    from .numeric_series_interface import NumericSeriesInterface
    from .sorted_series_interface import SortedSeriesInterface
    from .key_value_series_interface import KeyValueSeriesInterface

Native = Union[SortedSeriesInterface, KeyValueSeriesInterface]
SortedNumeric = Union[Native, NumericSeriesInterface]  # SortedNumericSeriesInterface
DateNumeric = Union[DateSeriesInterface, SortedNumeric]  # DateNumericSeriesInterface


class SortedKeyValueSeriesInterface(KeyValueSeriesInterface, SortedSeriesInterface, ABC):
    @abstractmethod
    def key_series(self, set_closure: bool = False, name: Name = None) -> SortedSeriesInterface:
        pass

    @abstractmethod
    def has_key_in_range(self, key: Any):
        pass

    @abstractmethod
    def get_first_key(self) -> Any:
        pass

    @abstractmethod
    def get_last_key(self) -> Any:
        pass

    @abstractmethod
    def get_first_item(self) -> tuple:
        pass

    @abstractmethod
    def get_last_item(self) -> tuple:
        pass

    @abstractmethod
    def get_border_keys(self) -> list:
        pass

    @abstractmethod
    def get_mutual_border_keys(self, other: Native) -> list:
        pass

    @abstractmethod
    def assume_sorted(self) -> Native:
        pass

    @abstractmethod
    def assume_unsorted(self) -> KeyValueSeriesInterface:
        pass

    @abstractmethod
    def assume_dates(self, validate: bool = False, set_closure: bool = False) -> DateNumeric:
        pass

    @abstractmethod
    def assume_numeric(self, validate: bool = False, set_closure: bool = False) -> SortedNumeric:
        pass

    @abstractmethod
    def to_numeric(self, sort_items: bool = True, inplace: bool = False) -> SortedNumeric:
        pass

    @abstractmethod
    def copy(self) -> Native:
        pass

    @abstractmethod
    def map_keys_and_values(
            self,
            key_function: Callable,
            value_function: Callable,
            sorting_changed: bool = False,
            inplace: bool = False,
    ) -> Native:
        pass

    @abstractmethod
    def exclude(self, first_key: Any, last_key: Any, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def span(self, first_key: Any, last_key: Any, inplace: bool = False) -> Native:
        pass
