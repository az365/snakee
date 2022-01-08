from typing import Optional, Callable, Iterable, Union

try:  # Assume we're a submodule in a package.
    from functions.primary import numeric as nm, dates as dt
    from series import series_classes as sc
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...functions.primary import numeric as nm, dates as dt
    from .. import series_classes as sc

Native = Union[sc.SortedSeries, sc.NumericSeries]
Series = sc.AnySeries

DEFAULT_NUMERIC = True
DEFAULT_SORTED = True


class SortedNumericSeries(sc.SortedSeries, sc.NumericSeries):
    def __init__(
            self,
            values: Optional[Iterable] = None,
            validate: bool = False,
            name: Optional[str] = None,
    ):
        super().__init__(
            values=values,
            validate=validate,
            name=name,
        )

    def get_errors(self) -> Iterable:
        if not self.is_sorted(check=True):
            yield 'Values of {} must be sorted'.format(self.get_class_name())
        if not self.has_valid_items():
            yield 'Values of {} must be numeric'.format(self.get_class_name())

    def assume_unsorted(self) -> sc.NumericSeries:
        return sc.NumericSeries(
            validate=False,
            **self._get_data_member_dict()
        )

    def to_dates(self, as_iso_date: bool = False, scale: dt.DateScale = dt.DateScale.Day) -> sc.SortedSeries:
        return self.map(
            function=lambda d: dt.get_date_from_int(d, scale=scale, as_iso_date=as_iso_date),
        ).assume_dates()

    def get_range_len(self) -> nm.NumericTypes:
        return self.get_distance_func()(
            *self.get_borders()
        )

    def distance(self, v: Union[Native, int, float], take_abs: bool = True) -> Union[Native, nm.NumericTypes]:
        got_one_value = isinstance(v, (int, float))
        if got_one_value:
            return self.distance_for_value(v, take_abs=take_abs)
        else:
            series = self.new(v, validate=False, sort_items=True)
            distance_series = sc.SortedNumericKeyValueSeries(
                self.get_values(),
                self.value_series().map(lambda i: series.get_distance_for_nearest_value(i, take_abs)),
                sort_items=False, validate=False,
            )
            return distance_series

    def distance_for_value(self, value: nm.NumericTypes, take_abs: bool = True) -> Native:
        distance_series = sc.SortedNumericKeyValueSeries(
            self.get_values(),
            self.value_series().map(lambda d: self.get_distance_func()(value, d, take_abs)),
        )
        return distance_series

    def get_distance_for_nearest_value(self, value: nm.NumericTypes, take_abs: bool = True) -> nm.NumericTypes:
        nearest_value = self.get_nearest_value(value)
        return self.value_series().get_distance_func()(value, nearest_value, take_abs)

    def get_nearest_value(self, value: nm.NumericTypes, distance_func: Optional[Callable] = None) -> nm.NumericTypes:
        distance_func = distance_func or self.get_distance_func()
        return super().get_nearest_value(value, distance_func)
