from typing import Optional, Callable, Iterable, Generator, Union

try:  # Assume we're a submodule in a package.
    from utils.decorators import deprecated_with_alternative
    from functions.primary import dates as dt
    from functions.secondary.date_functions import date_to_int, round_date, date_range
    from series.series_type import SeriesType
    from series.interfaces.sorted_numeric_series_interface import SortedNumericSeriesInterface
    from series.interfaces.date_series_interface import DateSeriesInterface, DateScale, Date, MAX_DAYS_IN_MONTH
    from series.interfaces.date_numeric_series_interface import DateNumericSeriesInterface, Name
    from series.simple.sorted_series import SortedSeries
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.decorators import deprecated_with_alternative
    from ...functions.primary import dates as dt
    from ...functions.secondary.date_functions import date_to_int, round_date, date_range
    from ..series_type import SeriesType
    from ..interfaces.sorted_numeric_series_interface import SortedNumericSeriesInterface
    from ..interfaces.date_series_interface import DateSeriesInterface, DateScale, Date, MAX_DAYS_IN_MONTH
    from ..interfaces.date_numeric_series_interface import DateNumericSeriesInterface, Name
    from .sorted_series import SortedSeries

Native = DateSeriesInterface

DEFAULT_NUMERIC = False
DEFAULT_SORTED = True


class DateSeries(SortedSeries, DateSeriesInterface):
    def __init__(
            self,
            values: Optional[Iterable] = None,
            caption: str = '',
            set_closure: bool = False,
            validate: bool = True,
            sort_items: bool = False,
            name: Name = None,
    ):
        super().__init__(
            values=values,
            caption=caption,
            set_closure=set_closure,
            validate=validate,
            sort_items=sort_items,
            name=name,
        )

    def get_series_type(self) -> SeriesType:
        return SeriesType.DateSeries

    def get_errors(self) -> Iterable:
        yield from super().get_errors()
        for i in self._get_invalid_examples():
            class_name = self.__class__.__name__
            yield f'Values of {class_name} must be python-dates or iso-dates, got {i}, ...'

    def _has_valid_dates(self) -> bool:
        for d in self.get_dates():
            if not self._is_valid_date(d):
                return False
        return True

    def _get_invalid_examples(self) -> Generator:
        invalid_types = set()
        for d in self.get_dates():
            if not self._is_valid_date(d):
                t = type(d)
                if t not in invalid_types:
                    yield d

    @staticmethod
    def _is_valid_date(date) -> bool:
        return dt.is_date(date, also_gost_format=False)

    @staticmethod
    def _is_native(series) -> bool:
        return isinstance(series, (DateSeries, DateSeriesInterface))

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
            return self.map_values(dt.get_date).get_values()
        else:
            return self.get_values()

    def set_dates(self, dates: Iterable, inplace: bool = False) -> Optional[Native]:
        result = self.set_values(dates, inplace=inplace)
        return self._assume_native(result) or self

    def to_dates(self, as_iso_date: bool = False) -> Native:
        return self.map_dates(
            lambda i: dt.get_date(i, as_iso_date=as_iso_date),
        )

    def to_int(self, scale: DateScale, inplace: bool = False) -> SortedNumericSeriesInterface:
        series = self.map_dates(date_to_int(scale=scale), inplace=inplace) or self
        return self._assume_sorted_numeric(series.assume_numeric())

    @deprecated_with_alternative('to_int(scale=DateScale.Day)')
    def to_days(self, inplace: bool = False) -> SortedNumericSeriesInterface:
        return self.to_int(scale=DateScale.Day, inplace=inplace)

    @deprecated_with_alternative('to_int(scale=DateScale.Week)')
    def to_weeks(self, inplace: bool = False) -> SortedNumericSeriesInterface:
        return self.to_int(scale=DateScale.Week, inplace=inplace)

    @deprecated_with_alternative('to_int(scale=DateScale.Month)')
    def to_months(self, inplace: bool = False) -> SortedNumericSeriesInterface:
        return self.to_int(scale=DateScale.Month, inplace=inplace)

    def to_years(self, decimal: bool = True, inplace: bool = False) -> SortedNumericSeriesInterface:
        series = self.map_dates(lambda d: dt.get_year_from_date(d, decimal=decimal), inplace=inplace) or self
        return self._assume_sorted_numeric(series.assume_numeric())

    def date_series(self, inplace: bool = False, set_closure: bool = False) -> Native:
        if inplace:
            return self
        else:
            return DateSeries(self.get_dates(), set_closure=set_closure, sort_items=False, validate=False)

    def get_first_date(self) -> Date:
        if self.has_items():
            return self.get_dates()[0]

    def get_last_date(self) -> Date:
        if self.has_items():
            return self.get_dates()[-1]

    def get_border_dates(self) -> list:
        return [self.get_first_date(), self.get_last_date()]

    def get_mutual_border_dates(self, other: Native) -> list:
        assert isinstance(other, (DateSeries, DateSeriesInterface, DateNumericSeriesInterface)), other
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

    def has_date_in_range(self, date: Date) -> bool:
        return self.get_first_date() <= date <= self.get_last_date()

    def map_dates(self, function: Callable, inplace: bool = False) -> Native:
        mapped_dates = map(function, self.get_dates())
        return self.set_dates(mapped_dates, inplace=inplace) or self

    def filter_dates(self, function: Callable, inplace: bool = False) -> Native:
        result = self.filter(function, inplace=inplace)
        return self._assume_native(result) or self

    def exclude(self, first_date: Date, last_date: Date, inplace: bool = False) -> Native:
        return self.filter_dates(lambda d: d < first_date or d > last_date, inplace=inplace) or self

    def period(self, first_date: Date, last_date: Date, inplace: bool = False) -> Native:
        return self.filter_dates(lambda d: first_date <= d <= last_date, inplace=inplace) or self

    def crop(self, left_days: int, right_days: int, inplace: bool = False) -> Native:
        first = dt.get_shifted_date(self.get_first_date(), days=abs(left_days))
        last = dt.get_shifted_date(self.get_last_date(), days=-abs(right_days))
        return self.period(first, last, inplace=inplace)

    def first_year(self, inplace: bool = False) -> Native:
        date_a = self.get_first_date()
        date_b = dt.get_next_year_date(date_a)
        return self.period(date_a, date_b, inplace=inplace)

    def last_year(self, inplace: bool = False) -> Native:
        date_b = self.get_last_date()
        date_a = dt.get_next_year_date(date_b, step=-1)
        return self.period(date_a, date_b, inplace=inplace)

    def shift(self, distance: int, inplace: bool = False) -> Native:
        return self.shift_dates(distance, inplace=inplace)

    def shift_dates(self, distance: int, inplace: bool = False) -> Native:
        return self.map_dates(lambda d: dt.get_shifted_date(d, days=distance), inplace=inplace)

    def yearly_shift(self, inplace: bool = False) -> Native:
        return self.map_dates(dt.get_next_year_date, inplace=inplace)

    def round_to(self, scale: DateScale, as_iso_date: Optional[bool] = None, inplace: bool = False) -> Native:
        func = round_date(scale, as_iso_date=as_iso_date)
        series = self.map_dates(func, inplace=inplace).uniq(inplace=inplace) or self
        return self._assume_native(series)

    @deprecated_with_alternative('round_to(scale=DateScale.Week')
    def round_to_weeks(self, inplace: bool = False) -> Native:
        return self.round_to(DateScale.Week, inplace=inplace)

    @deprecated_with_alternative('round_to(scale=DateScale.Month')
    def round_to_months(self, inplace: bool = False) -> Native:
        return self.round_to(DateScale.Month, inplace=inplace)

    def distance(self, d: [Native, Date], take_abs: bool = True, inplace: bool = False) -> DateNumericSeriesInterface:
        if dt.is_date(d):
            distance_series = self.distance_for_date(d, take_abs=take_abs)
        elif self._is_native(d):
            arg_date_series = self.make_new(d, validate=False, sort_items=False, set_closure=True)
            self_date_series = self.date_series(inplace=inplace, set_closure=inplace)
            func = arg_date_series.get_distance_for_nearest_date
            distances = self_date_series.map(lambda i: func(i, take_abs), inplace=inplace) or self_date_series
            if inplace:
                distance_series = distances
            else:
                series_class = SeriesType.DateNumericSeries.get_class()
                distance_series = series_class(self.get_dates(), distances, sort_items=False, validate=False)
        else:
            raise TypeError(f'd-argument for distance-method must be date or DateSeries (got {d}: {d})')
        return distance_series

    def distance_for_date(
            self,
            date: Date,
            take_abs: bool = True,
            set_closure: bool = False,
            name: Name = None,
    ) -> DateNumericSeriesInterface:
        f = self.get_distance_func()
        dates = self.get_dates()
        values = self.date_series(set_closure=set_closure).map(lambda d: f(date, d, take_abs), inplace=not set_closure)
        series_class = SeriesType.DateNumericSeries.get_class()
        return series_class(dates, values, sort_items=False, validate=False, set_closure=set_closure)

    def get_distance_for_nearest_date(self, date: Date, take_abs: bool = True) -> int:
        nearest_date = self.get_nearest_date(date)
        return self.get_distance_func()(date, nearest_date, take_abs)

    def get_nearest_date(self, date: Date, distance_func: Optional[Callable] = None) -> Date:
        if distance_func is None:
            distance_func = self.get_distance_func()
        return self.date_series().get_nearest_value(date, distance_func=distance_func)

    def get_two_nearest_dates(self, date: Date) -> Optional[tuple]:
        if self.get_count() < 2:
            return None
        else:
            distance_series = self.date_series().distance(date, take_abs=False)
            date_a = distance_series.filter_values(lambda v: v < 0).get_arg_max()
            date_b = distance_series.filter_values(lambda v: v >= 0).get_arg_min()
            return date_a, date_b

    def get_segment(self, date: Date, inplace: bool = False) -> Native:
        nearest_dates = [i for i in self.get_two_nearest_dates(date) if i]
        return self.set_dates(nearest_dates, inplace=inplace) or self

    def interpolate_to_scale(self, scale: DateScale, inplace: bool = False) -> Native:
        func = date_range(scale=scale)
        dates = func(self.get_first_date(), self.get_last_date())
        return self.set_dates(dates, inplace=inplace) or self

    @deprecated_with_alternative('interpolate_to_weeks(scale=DateScale.Week)')
    def interpolate_to_weeks(self, inplace: bool = False) -> Native:
        return self.interpolate_to_scale(DateScale.Week, inplace=inplace)

    @deprecated_with_alternative('interpolate_to_weeks(scale=DateScale.Month)')
    def interpolate_to_months(self, inplace: bool = False) -> Native:
        return self.interpolate_to_scale(DateScale.Month, inplace=inplace)

    def find_base_date(
            self, date: Date,
            max_distance: int = MAX_DAYS_IN_MONTH, return_increment: bool = False,
    ) -> Union[Date, tuple, None]:
        candidates = DateSeries(
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

    @staticmethod
    def _assume_native(series) -> Native:
        return series

    @staticmethod
    def _assume_sorted_numeric(series) -> SortedNumericSeriesInterface:
        return series


SeriesType.add_classes(SortedSeries, DateSeries)
