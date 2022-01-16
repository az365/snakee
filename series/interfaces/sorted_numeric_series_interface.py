from abc import ABC, abstractmethod
from typing import Optional, Callable, Union

try:  # Assume we're a submodule in a package.
    from functions.primary import dates as dt
    from series.interfaces.sorted_series_interface import SortedSeriesInterface
    from series.interfaces.numeric_series_interface import NumericSeriesInterface, NumericValue
    from series.interfaces.date_series_interface import DateSeriesInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...functions.primary import dates as dt
    from .sorted_series_interface import SortedSeriesInterface
    from .numeric_series_interface import NumericSeriesInterface, NumericValue
    from .date_series_interface import DateSeriesInterface

Native = Union[SortedSeriesInterface, NumericSeriesInterface]


class SortedNumericSeriesInterface(SortedSeriesInterface, NumericSeriesInterface, ABC):
    @abstractmethod
    def assume_unsorted(self) -> NumericSeriesInterface:
        pass

    @abstractmethod
    def to_dates(
            self,
            as_iso_date: bool = False,
            scale: dt.DateScale = dt.DateScale.Day,
            set_closure: bool = False,
            inplace: bool = False,
    ) -> DateSeriesInterface:
        pass

    @abstractmethod
    def get_range_len(self) -> NumericValue:
        pass

    @abstractmethod
    def distance(
            self,
            v: Union[Native, NumericValue],
            take_abs: bool = True,
            inplace: bool = False,
    ) -> Union[Native, NumericValue]:
        pass

    @abstractmethod
    def distance_for_value(self, value: NumericValue, take_abs: bool = True, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def get_distance_for_nearest_value(self, value: NumericValue, take_abs: bool = True) -> NumericValue:
        pass

    @abstractmethod
    def get_nearest_value(self, value: NumericValue, distance_func: Optional[Callable] = None) -> NumericValue:
        pass
