from typing import Optional, Callable, Iterable, Generator, Union

try:  # Assume we're a submodule in a package.
    from functions.primary import numeric as nm, dates as dt
    from series.series_type import SeriesType
    from series.interfaces.any_series_interface import AnySeriesInterface
    from series.interfaces.date_series_interface import DateSeriesInterface
    from series.interfaces.key_value_series_interface import KeyValueSeriesInterface
    from series.simple.sorted_series import SortedSeries
    from series.simple.numeric_series import NumericSeries
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...functions.primary import numeric as nm, dates as dt
    from ..series_type import SeriesType
    from ..interfaces.any_series_interface import AnySeriesInterface
    from ..interfaces.date_series_interface import DateSeriesInterface
    from ..interfaces.key_value_series_interface import KeyValueSeriesInterface
    from .sorted_series import SortedSeries
    from .numeric_series import NumericSeries

Native = Union[SortedSeries, NumericSeries]
Series = Union[Native, AnySeriesInterface, DateSeriesInterface, KeyValueSeriesInterface]
NumericValue = nm.NumericTypes

DEFAULT_NUMERIC = True
DEFAULT_SORTED = True


class SortedNumericSeries(SortedSeries, NumericSeries):
    def __init__(
            self,
            values: Optional[Iterable] = None,
            set_closure: bool = False,
            validate: bool = False,
            name: Optional[str] = None,
    ):
        super().__init__(values=values, set_closure=set_closure, validate=validate, name=name)

    def get_errors(self) -> Generator:
        if not self.is_sorted(check=True):
            yield 'Values of {} must be sorted'.format(self.get_class_name())
        if not self.has_valid_items():
            yield 'Values of {} must be numeric'.format(self.get_class_name())

    def assume_unsorted(self) -> NumericSeries:
        series_class = SeriesType.NumericSeries.get_class()
        return series_class(**self._get_data_member_dict(), validate=False)

    def to_dates(
            self,
            as_iso_date: bool = False,
            scale: dt.DateScale = dt.DateScale.Day,
            set_closure: bool = False,
            inplace: bool = False,
    ) -> SortedSeries:
        mapped_series = self.map(
            function=lambda d: dt.get_date_from_int(d, scale=scale, as_iso_date=as_iso_date),
            validate=False, inplace=inplace,
        ) or self
        date_series = mapped_series.assume_dates(set_closure=set_closure, validate=False)
        return self._assume_sorted(date_series)

    def get_range_len(self) -> NumericValue:
        distance_func = self.get_distance_func()
        return distance_func(*self.get_borders())

    def distance(
            self,
            v: Union[Native, NumericValue],
            take_abs: bool = True,
            inplace: bool = False,
    ) -> Union[Native, NumericValue]:
        got_one_value = isinstance(v, nm.NUMERIC_TYPES)
        if got_one_value:
            return self.distance_for_value(v, take_abs=take_abs, inplace=inplace)
        else:
            v_series = self.new(v, validate=False, sort_items=True)
            distances = self.value_series().map(lambda i: v_series.get_distance_for_nearest_value(i, take_abs))
            series_class = SeriesType.SortedNumericKeyValueSeries.get_class()
            result = series_class(self.get_values(), distances, sort_items=False, validate=False)
            if inplace:
                self.set_values(result.get_values(), inplace=True)
            return result

    def distance_for_value(self, value: NumericValue, take_abs: bool = True, inplace: bool = False) -> Native:
        distance_func = self.get_distance_func()
        distance_series = self.value_series().map(lambda d: distance_func(value, d, take_abs), inplace=inplace)
        series_class = SeriesType.SortedNumericKeyValueSeries.get_class()
        result = series_class(self.get_values(), distance_series)
        if inplace:
            self.set_values(result.get_values(), inplace=True)
        return result

    def get_distance_for_nearest_value(self, value: NumericValue, take_abs: bool = True) -> NumericValue:
        nearest_value = self.get_nearest_value(value)
        distance_func = self.value_series().get_distance_func()
        return distance_func(value, nearest_value, take_abs)

    def get_nearest_value(self, value: NumericValue, distance_func: Optional[Callable] = None) -> NumericValue:
        distance_func = distance_func or self.get_distance_func()
        return super().get_nearest_value(value, distance_func)

    @staticmethod
    def _assume_sorted(series) -> SortedSeries:
        return series


SeriesType.add_classes(SortedSeries, NumericSeries, SortedNumericSeries)
