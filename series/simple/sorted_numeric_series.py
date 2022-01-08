from typing import Optional, Callable, Iterable, Generator, Union

try:  # Assume we're a submodule in a package.
    from functions.primary import numeric as nm, dates as dt
    from series import series_classes as sc
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...functions.primary import numeric as nm, dates as dt
    from .. import series_classes as sc

Native = Union[sc.SortedSeries, sc.NumericSeries]
Series = sc.AnySeries
SortedS = sc.SortedSeries
NumericS = sc.NumericSeries
NumericValue = nm.NumericTypes

DEFAULT_NUMERIC = True
DEFAULT_SORTED = True


class SortedNumericSeries(sc.SortedSeries, sc.NumericSeries):
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

    def assume_unsorted(self) -> NumericS:
        return sc.NumericSeries(
            validate=False,
            **self._get_data_member_dict()
        )

    def to_dates(
            self,
            as_iso_date: bool = False,
            scale: dt.DateScale = dt.DateScale.Day,
            set_closure: bool = False,
            inplace: bool = False,
    ) -> SortedS:
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
            series = self.new(v, validate=False, sort_items=True)
            distance_series = sc.SortedNumericKeyValueSeries(
                self.get_values(),
                self.value_series().map(lambda i: series.get_distance_for_nearest_value(i, take_abs)),
                sort_items=False, validate=False,
            )
            if inplace:
                self.set_values(distance_series.get_values(), inplace=True)
            return distance_series

    def distance_for_value(self, value: NumericValue, take_abs: bool = True, inplace: bool = False) -> Native:
        distance_func = self.get_distance_func()
        distance_series = sc.SortedNumericKeyValueSeries(
            self.get_values(),
            self.value_series().map(lambda d: distance_func(value, d, take_abs), inplace=inplace),
        )
        if inplace:
            self.set_values(distance_series.get_values(), inplace=True)
        return distance_series

    def get_distance_for_nearest_value(self, value: NumericValue, take_abs: bool = True) -> NumericValue:
        nearest_value = self.get_nearest_value(value)
        distance_func = self.value_series().get_distance_func()
        return distance_func(value, nearest_value, take_abs)

    def get_nearest_value(self, value: NumericValue, distance_func: Optional[Callable] = None) -> NumericValue:
        distance_func = distance_func or self.get_distance_func()
        return super().get_nearest_value(value, distance_func)

    @staticmethod
    def _assume_sorted(series) -> SortedS:
        return series
