from abc import ABC, abstractmethod
from typing import Callable, Iterable, Optional, Union

try:  # Assume we're a submodule in a package.
    # from base.interfaces.iterable_interface import IterableInterface
    from functions.primary import dates as dt
    from series.interfaces.any_series_interface import AnySeriesInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    # from ...base.interfaces.iterable_interface import IterableInterface
    from ...functions.primary import dates as dt
    from .any_series_interface import AnySeriesInterface

Native = AnySeriesInterface
SeriesInterface = AnySeriesInterface
SortedNumeric = AnySeriesInterface
DateNumeric = AnySeriesInterface


class DateSeriesInterface(AnySeriesInterface, ABC):
    @staticmethod
    @abstractmethod
    def get_distance_func() -> Callable:
        return dt.get_days_between

    @abstractmethod
    def is_dates(self, check: bool = False) -> bool:
        pass

    @abstractmethod
    def get_dates(self, as_date_type: bool = False) -> list:
        pass

    @abstractmethod
    def set_dates(self, dates: Iterable) -> Native:
        pass

    @abstractmethod
    def to_days(self) -> SortedNumeric:
        pass

    @abstractmethod
    def to_weeks(self) -> SortedNumeric:
        pass

    @abstractmethod
    def to_months(self) -> SortedNumeric:
        pass

    @abstractmethod
    def to_years(self) -> SortedNumeric:
        pass

    @abstractmethod
    def date_series(self) -> Native:
        pass

    @abstractmethod
    def get_first_date(self) -> dt.Date:
        pass

    @abstractmethod
    def get_last_date(self) -> dt.Date:
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
    def has_date_in_range(self, date: dt.Date) -> bool:
        pass

    @abstractmethod
    def map_dates(self, function: Callable) -> Native:
        pass

    @abstractmethod
    def filter_dates(self, function: Callable) -> Native:
        pass

    @abstractmethod
    def exclude(self, first_date: dt.Date, last_date: dt.Date) -> Native:
        pass

    @abstractmethod
    def period(self, first_date: dt.Date, last_date: dt.Date) -> Native:
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
    def round_to_weeks(self) -> Native:
        pass

    @abstractmethod
    def round_to_months(self) -> Native:
        pass

    @abstractmethod
    def distance(self, d: dt.Date, take_abs: bool = True) -> DateNumeric:
        pass

    @abstractmethod
    def distance_for_date(self, date: dt.Date, take_abs: bool = True) -> DateNumeric:
        pass

    @abstractmethod
    def get_distance_for_nearest_date(self, date: dt.Date, take_abs: bool = True) -> int:
        pass

    @abstractmethod
    def get_nearest_date(self, date: dt.Date, distance_func: Optional[Callable] = None) -> dt.Date:
        pass

    @abstractmethod
    def get_two_nearest_dates(self, date: dt.Date) -> Optional[tuple]:
        pass

    @abstractmethod
    def get_segment(self, date: dt.Date) -> Native:
        pass

    @abstractmethod
    def interpolate_to_weeks(self) -> Native:
        pass

    @abstractmethod
    def interpolate_to_months(self) -> Native:
        pass

    @abstractmethod
    def find_base_date(
            self, date: dt.Date,
            max_distance: int = dt.MAX_DAYS_IN_MONTH, return_increment: bool = False,
    ) -> Union[dt.Date, tuple]:
        pass
