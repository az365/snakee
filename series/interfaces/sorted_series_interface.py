from abc import ABC, abstractmethod
from typing import Optional, Callable, Union, Any

try:  # Assume we're a submodule in a package.
    from functions.primary import dates as dt
    from series.series_type import SeriesType
    from series.interfaces.any_series_interface import AnySeriesInterface
    from series.interfaces.numeric_series_interface import NumericSeriesInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...functions.primary import dates as dt
    from ..series_type import SeriesType
    from .any_series_interface import AnySeriesInterface
    from .numeric_series_interface import NumericSeriesInterface

Native = AnySeriesInterface
SortedNumeric = Union[Native, NumericSeriesInterface]  # SortedNumericSeriesInterface


class SortedSeriesInterface(AnySeriesInterface, ABC):
    @abstractmethod
    def copy(self) -> Native:
        pass

    @abstractmethod
    def assume_numeric(self, validate: bool = False) -> SortedNumeric:
        pass

    @abstractmethod
    def assume_unsorted(self) -> AnySeriesInterface:
        pass

    @abstractmethod
    def uniq(self) -> Native:
        pass

    @abstractmethod
    def get_nearest_value(self, value: Any, distance_func: Callable) -> Any:
        pass

    @abstractmethod
    def get_two_nearest_values(self, value: Any) -> Optional[tuple]:
        pass

    @abstractmethod
    def get_first_value(self) -> Any:
        pass

    @abstractmethod
    def get_last_value(self) -> Any:
        pass

    @abstractmethod
    def get_first_item(self) -> Any:
        pass

    @abstractmethod
    def get_last_item(self) -> Any:
        pass

    @abstractmethod
    def get_borders(self) -> tuple:
        pass

    @abstractmethod
    def get_mutual_borders(self, other: Native) -> list:
        pass

    @abstractmethod
    def borders(self, other: Native = None) -> Native:
        pass
