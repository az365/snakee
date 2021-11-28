from abc import abstractmethod
from typing import Iterable, Callable, Optional, Union

try:  # Assume we're a sub-module in a package.
    from series import series_classes as sc
    from functions.primary import dates as dt
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import series_classes as sc
    from ...utils import dates as dt

NativeInterface = sc.SortedSeries
DateNumericInterface = NativeInterface
SortedNumeric = sc.SortedNumericSeries
Series = sc.AnySeries

DEFAULT_NUMERIC = False
DEFAULT_SORTED = True


class DateSeriesInterface(NativeInterface):
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
    def set_dates(self, dates: Iterable) -> NativeInterface:
        pass

    @abstractmethod
    def to_days(self) -> SortedNumeric:
        pass

    @abstractmethod
    def to_weeks(self) -> SortedNumeric:
        pass

    @abstractmethod
    def to_years(self) -> SortedNumeric:
        pass

    @abstractmethod
    def date_series(self) -> NativeInterface:
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
    def get_mutual_border_dates(self, other: NativeInterface) -> list:
        pass

    @abstractmethod
    def border_dates(self, other: Optional[NativeInterface] = None) -> NativeInterface:
        pass

    @abstractmethod
    def get_range_len(self) -> int:
        pass

    @abstractmethod
    def has_date_in_range(self, date: dt.Date) -> bool:
        pass

    @abstractmethod
    def map_dates(self, function: Callable) -> NativeInterface:
        pass

    @abstractmethod
    def filter_dates(self, function: Callable) -> NativeInterface:
        pass

    @abstractmethod
    def exclude(self, first_date: dt.Date, last_date: dt.Date) -> NativeInterface:
        pass

    @abstractmethod
    def period(self, first_date: dt.Date, last_date: dt.Date) -> NativeInterface:
        pass

    @abstractmethod
    def first_year(self) -> NativeInterface:
        pass

    @abstractmethod
    def last_year(self) -> NativeInterface:
        pass

    @abstractmethod
    def shift_dates(self, distance: int) -> NativeInterface:
        pass

    @abstractmethod
    def yearly_shift(self) -> NativeInterface:
        pass

    @abstractmethod
    def round_to_weeks(self) -> NativeInterface:
        pass

    @abstractmethod
    def round_to_months(self) -> NativeInterface:
        pass

    @abstractmethod
    def distance(self, d: dt.Date, take_abs: bool = True) -> DateNumericInterface:
        pass

    @abstractmethod
    def distance_for_date(self, date: dt.Date, take_abs: bool = True) -> DateNumericInterface:
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
    def get_segment(self, date: dt.Date) -> NativeInterface:
        pass

    @abstractmethod
    def interpolate_to_weeks(self) -> NativeInterface:
        pass

    @abstractmethod
    def find_base_date(
            self, date: dt.Date,
            max_distance: int = dt.MAX_DAYS_IN_MONTH, return_increment: bool = False,
    ) -> Union[dt.Date, tuple]:
        pass


Native = DateSeriesInterface
DateNumeric = Native


class DateSeries(DateSeriesInterface, sc.SortedSeries):
    def __init__(
            self,
            values: Optional[list] = None,
            validate: bool = True,
            sort_items: bool = False,
            name=None,
    ):
        super().__init__(
            values=values or list(),
            validate=validate,
            sort_items=sort_items,
            name=name,
        )

    def get_errors(self) -> Iterable:
        yield from super().get_errors()
        if not self._has_valid_dates():
            yield 'Values of {} must be python-dates or iso-dates'.format(self.get_class_name())

    def _has_valid_dates(self) -> bool:
        for d in self.get_dates():
            if not self._is_valid_date(d):
                return False
        return True

    @staticmethod
    def _is_valid_date(date) -> bool:
        return dt.is_date(date, also_gost_format=False)

    @staticmethod
    def _is_native(series) -> bool:
        return isinstance(series, (DateSeries, sc.DateSeries))

    @staticmethod
    def get_distance_func() -> Callable:
        return dt.get_days_between

    @staticmethod
    def is_numeric(*_) -> bool:
        return DEFAULT_NUMERIC

    def is_dates(self, check: bool = False) -> bool:
        if check:
            return self._has_valid_dates()
        else:
            return True

    def get_dates(self, as_date_type: bool = False) -> list:
        if as_date_type:
            return self.map_values(dt.to_date).get_values()
        else:
            return self.get_values()

    def set_dates(self, dates: Iterable, inplace: bool = False) -> Optional[Native]:
        return self.set_values(dates, inplace=inplace)

    def to_dates(self, as_iso_date: bool = False) -> Native:
        return self.map_dates(
            lambda i: dt.to_date(i, as_iso_date=as_iso_date),
        )

    def to_days(self) -> SortedNumeric:
        return self.map_dates(dt.get_day_abs_from_date).assume_numeric()

    def to_weeks(self) -> SortedNumeric:
        return self.map_dates(dt.get_week_abs_from_date).assume_numeric()

    def to_years(self) -> SortedNumeric:
        return self.map_dates(lambda d: dt.get_year_from_date(d, decimal=True)).assume_numeric()

    def date_series(self) -> Native:
        return DateSeries(
            self.get_dates(),
            sort_items=False,
            validate=False,
        )

    def get_first_date(self) -> dt.Date:
        if self.has_items():
            return self.get_dates()[0]

    def get_last_date(self) -> dt.Date:
        if self.has_items():
            return self.get_dates()[-1]

    def get_border_dates(self) -> list:
        return [self.get_first_date(), self.get_last_date()]

    def get_mutual_border_dates(self, other: Native) -> list:
        assert isinstance(other, (sc.DateSeries, sc.DateNumericSeries))
        first_date = max(self.get_first_date(), other.get_first_date())
        last_date = min(self.get_last_date(), other.get_last_date())
        if first_date < last_date:
            return [first_date, last_date]

    def border_dates(self, other: Optional[Native] = None) -> Native:
        if other:
            return DateSeries(self.get_mutual_border_dates(other))
        else:
            return DateSeries(self.get_border_dates())

    def get_range_len(self) -> int:
        return self.get_distance_func()(
            *self.get_border_dates()
        )

    def has_date_in_range(self, date: dt.Date) -> bool:
        return self.get_first_date() <= date <= self.get_last_date()

    def map_dates(self, function: Callable) -> Native:
        return self.set_dates(
            map(function, self.get_dates()),
        )

    def filter_dates(self, function: Callable) -> Native:
        return self.filter(function)

    def exclude(self, first_date: dt.Date, last_date: dt.Date) -> Native:
        return self.filter_dates(lambda d: d < first_date or d > last_date)

    def period(self, first_date: dt.Date, last_date: dt.Date) -> Native:
        return self.filter_dates(lambda d: first_date <= d <= last_date)

    def crop(self, left_days: int, right_days: int) -> Native:
        return self.period(
            dt.get_shifted_date(self.get_first_date(), days=abs(left_days)),
            dt.get_shifted_date(self.get_last_date(), days=-abs(right_days)),
        )

    def first_year(self) -> Native:
        date_a = self.get_first_date()
        date_b = dt.get_next_year_date(date_a)
        return self.period(date_a, date_b)

    def last_year(self) -> Native:
        date_b = self.get_last_date()
        date_a = dt.get_next_year_date(date_b, step=-1)
        return self.period(date_a, date_b)

    def shift(self, distance: int) -> Native:
        return self.shift_dates(distance)

    def shift_dates(self, distance: int) -> Native:
        return self.map_dates(lambda d: dt.get_shifted_date(d, days=distance))

    def yearly_shift(self) -> Native:
        return self.map_dates(dt.get_next_year_date)

    def round_to_weeks(self) -> Native:
        return self.map_dates(dt.get_monday_date).uniq()

    def round_to_months(self) -> Native:
        return self.map_dates(dt.get_month_first_date).uniq()

    def distance(self, d: [Native, dt.Date], take_abs: bool = True) -> DateNumeric:
        if dt.is_date(d):
            distance_series = self.distance_for_date(d, take_abs=take_abs)
        elif self._is_native(d):
            date_series = self.new(d, validate=False, sort_items=False)
            distance_series = sc.DateNumericSeries(
                self.get_dates(),
                self.date_series().map(lambda i: date_series.get_distance_for_nearest_date(i, take_abs)),
                sort_items=False, validate=False,
            )
        else:
            raise TypeError('d-argument for distance-method must be date or DateSeries (got {}: {})'.format(type(d), d))
        return distance_series

    def distance_for_date(self, date: dt.Date, take_abs: bool = True) -> DateNumeric:
        return sc.DateNumericSeries(
            self.get_dates(),
            self.date_series().map(lambda d: self.get_distance_func()(date, d, take_abs)),
            sort_items=False, validate=False,
        )

    def get_distance_for_nearest_date(self, date: dt.Date, take_abs: bool = True) -> int:
        nearest_date = self.get_nearest_date(date)
        return self.get_distance_func()(date, nearest_date, take_abs)

    def get_nearest_date(self, date: dt.Date, distance_func: Optional[Callable] = None) -> dt.Date:
        return self.date_series().get_nearest_value(
            date,
            distance_func=distance_func or self.get_distance_func(),
        )

    def get_two_nearest_dates(self, date: dt.Date) -> Optional[tuple]:
        if self.get_count() < 2:
            return None
        else:
            distance_series = self.date_series().distance(date, take_abs=False)
            date_a = distance_series.filter_values(lambda v: v < 0).get_arg_max()
            date_b = distance_series.filter_values(lambda v: v >= 0).get_arg_min()
            return date_a, date_b

    def get_segment(self, date: dt.Date) -> Native:
        nearest_dates = [i for i in self.get_two_nearest_dates(date) if i]
        return self.new(nearest_dates)

    def interpolate_to_weeks(self) -> Native:
        monday_dates = dt.get_weeks_range(self.get_first_date(), self.get_last_date())
        return self.new(monday_dates)

    def find_base_date(
            self, date: dt.Date,
            max_distance: int = dt.MAX_DAYS_IN_MONTH, return_increment: bool = False,
    ) -> Union[dt.Date, Optional[tuple]]:
        candidates = sc.DateSeries(
            dt.get_yearly_dates(date, self.get_first_date(), self.get_last_date()),
        )
        if not candidates.has_items():
            return None
        distance_for_base_date = candidates.distance(self.date_series())
        filtered_distances_for_base_date = distance_for_base_date.filter_values(lambda d: abs(d) <= max_distance)
        if filtered_distances_for_base_date.has_items():
            distance_for_cur_date = filtered_distances_for_base_date.distance(date, take_abs=True)
        else:
            distance_for_cur_date = distance_for_base_date.distance(date, take_abs=True)
        base_date = distance_for_cur_date.get_arg_min()
        if return_increment:
            increment = self.get_distance_func()(base_date, date, take_abs=False) / dt.DAYS_IN_YEAR
            increment = round(increment)
            increment = int(increment)
            return base_date, increment
        else:
            return base_date
