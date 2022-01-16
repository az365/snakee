from abc import ABC, abstractmethod
from typing import Callable, Iterable, Optional, Union

try:  # Assume we're a submodule in a package.
    from utils.arguments import Auto, AUTO
    from functions.primary.dates import Date, DateScale, MAX_DAYS_IN_MONTH
    from series.interfaces.any_series_interface import AnySeriesInterface
    from series.interfaces.numeric_series_interface import NumericSeriesInterface
    from series.interfaces.sorted_series_interface import SortedSeriesInterface
    from series.interfaces.key_value_series_interface import KeyValueSeriesInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.arguments import Auto, AUTO
    from ...functions.primary.dates import Date, DateScale, MAX_DAYS_IN_MONTH
    from .any_series_interface import AnySeriesInterface
    from .numeric_series_interface import NumericSeriesInterface
    from .sorted_series_interface import SortedSeriesInterface
    from .key_value_series_interface import KeyValueSeriesInterface

Native = SortedSeriesInterface
SeriesInterface = AnySeriesInterface
SortedNumeric = Union[SortedSeriesInterface, NumericSeriesInterface]
DateNumeric = Union[Native, KeyValueSeriesInterface, NumericSeriesInterface]
AutoBool = Union[Auto, bool]


class DateSeriesInterface(SortedSeriesInterface, ABC):
    @staticmethod
    @abstractmethod
    def get_distance_func() -> Callable:
        pass

    @abstractmethod
    def is_dates(self, check: bool = False) -> bool:
        pass

    @abstractmethod
    def get_dates(self, as_date_type: Optional[bool] = False) -> list:
        pass

    @abstractmethod
    def set_dates(self, dates: Iterable) -> Native:
        pass

    @abstractmethod
    def to_int(self, scale: DateScale, inplace: bool = False) -> SortedNumeric:
        pass

    @abstractmethod
    def date_series(self) -> Native:
        pass

    @abstractmethod
    def get_first_date(self) -> Date:
        pass

    @abstractmethod
    def get_last_date(self) -> Date:
        pass

    @abstractmethod
    def get_border_dates(self) -> list:
        pass

    @abstractmethod
    def get_mutual_border_dates(self, other: Native) -> list:
        pass

    @abstractmethod
    def border_dates(self, other: Optional[Native] = None) -> Native:
        pass

    @abstractmethod
    def get_range_len(self) -> int:
        pass

    @abstractmethod
    def has_date_in_range(self, date: Date) -> bool:
        pass

    @abstractmethod
    def map_dates(self, function: Callable) -> Native:
        pass

    @abstractmethod
    def filter_dates(self, function: Callable) -> Native:
        pass

    @abstractmethod
    def exclude(self, first_date: Date, last_date: Date) -> Native:
        pass

    @abstractmethod
    def period(self, first_date: Date, last_date: Date) -> Native:
        pass

    @abstractmethod
    def first_year(self) -> Native:
        pass

    @abstractmethod
    def last_year(self) -> Native:
        pass

    @abstractmethod
    def shift_dates(self, distance: int) -> Native:
        pass

    @abstractmethod
    def yearly_shift(self) -> Native:
        pass

    @abstractmethod
    def round_to(self, scale: DateScale, as_iso_date: AutoBool = AUTO, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def distance(self, d: Date, take_abs: bool = True) -> DateNumeric:
        pass

    @abstractmethod
    def distance_for_date(self, date: Date, take_abs: bool = True) -> DateNumeric:
        pass

    @abstractmethod
    def get_distance_for_nearest_date(self, date: Date, take_abs: bool = True) -> int:
        pass

    @abstractmethod
    def get_nearest_date(self, date: Date, distance_func: Optional[Callable] = None) -> Date:
        pass

    @abstractmethod
    def get_two_nearest_dates(self, date: Date) -> Optional[tuple]:
        pass

    @abstractmethod
    def get_segment(self, date: Date) -> Native:
        pass

    @abstractmethod
    def interpolate_to_scale(self, scale: DateScale, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def find_base_date(
            self, date: Date,
            max_distance: int = MAX_DAYS_IN_MONTH,
            return_increment: bool = False,
    ) -> Union[Date, tuple]:
        pass
