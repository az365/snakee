from typing import Optional, Callable, Iterable, Generator, Any

try:  # Assume we're a submodule in a package.
    from series.series_type import SeriesType
    from series.interfaces.sorted_series_interface import SortedSeriesInterface
    from series.interfaces.sorted_numeric_series_interface import SortedNumericSeriesInterface
    from series.interfaces.sorted_key_value_series_interface import SortedKeyValueSeriesInterface, Name
    from series.simple.any_series import AnySeries
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..series_type import SeriesType
    from ..interfaces.sorted_series_interface import SortedSeriesInterface
    from ..interfaces.sorted_numeric_series_interface import SortedNumericSeriesInterface
    from ..interfaces.sorted_key_value_series_interface import SortedKeyValueSeriesInterface, Name
    from .any_series import AnySeries

Native = SortedSeriesInterface

DEFAULT_NUMERIC = False
DEFAULT_SORTED = True


class SortedSeries(AnySeries, SortedSeriesInterface):
    def __init__(
            self,
            values: Optional[Iterable] = None,
            set_closure: bool = False,
            validate: bool = True,
            sort_items: bool = False,
            name: Name = None,
    ):
        if sort_items:
            values = sorted(values)
        super().__init__(values=values, set_closure=set_closure, validate=validate, name=name)

    def get_series_type(self) -> SeriesType:
        return SeriesType.SortedSeries

    def get_errors(self) -> Generator:
        yield from super().get_errors()
        if not self.is_sorted(check=True):
            yield 'Values of {} must be sorted'.format(self.get_class_name())

    def is_sorted(self, check: bool = False) -> bool:
        if check:
            return super().is_sorted(check=True)
        else:
            return DEFAULT_SORTED

    def copy(self, validate: bool = False, **kwargs) -> Native:
        series = self.new(validate=validate, **kwargs)
        return self._assume_native(series)

    def assume_numeric(self, validate: bool = False) -> SortedNumericSeriesInterface:
        series_class = SeriesType.SortedNumericSeries.get_class()
        return series_class(**self._get_data_member_dict(), validate=validate)

    def assume_sorted(self) -> Native:
        return self

    def assume_unsorted(self) -> AnySeries:
        series_class = SeriesType.AnySeries.get_class()
        return series_class(**self._get_data_member_dict())

    def uniq(self, inplace: bool = False) -> Native:
        prev = None
        if inplace:
            values = self.get_values()
            n = 0
            while n < self.get_count():
                item = values[n]
                if item == prev:
                    del values[n]
                else:
                    n += 1
            return self
        else:
            series = self.new(save_meta=True)
            series = self._assume_native(series)
            for item in self.get_items():
                if prev is None or item != prev:
                    series.append(item, inplace=True)
                if hasattr(item, 'copy'):
                    prev = item.copy()
                else:
                    prev = item
            return self._assume_native(series)

    def get_nearest_value(self, value: Any, distance_func: Callable) -> Any:
        if self.get_count() == 0:
            return None
        elif self.get_count() == 1:
            return self.get_first_value()
        else:
            cur_value = None
            prev_value = None
            prev_distance = None
            for cur_value in self.get_values():
                if cur_value == value:
                    return cur_value
                else:
                    cur_distance = abs(distance_func(cur_value, value))
                    if prev_distance is not None:
                        if cur_distance > prev_distance:
                            return prev_value
                    prev_value = cur_value
                    prev_distance = cur_distance
            return cur_value

    def get_two_nearest_values(self, value: Any) -> Optional[tuple]:
        if self.get_count() < 2:
            return None
        elif hasattr(self, 'distance'):  # isinstance(self, (DateSeries, SortedNumericSeries)):
            distance_series = self.distance(value, take_abs=False)
            date_a = distance_series.filter_values(lambda v: v < 0).get_arg_max()
            date_b = distance_series.filter_values(lambda v: v >= 0).get_arg_min()
            return date_a, date_b
        else:
            msg = 'get_two_nearest_values() method available for DateSeries, SortedNumericSeries only, got {}'
            raise TypeError(msg.format(self))

    def get_first_value(self) -> Any:
        if self.get_count():
            return self.get_values()[0]

    def get_last_value(self) -> Any:
        if self.get_count():
            return self.get_values()[-1]

    def get_first_item(self) -> Any:
        return self.get_first_value()

    def get_last_item(self) -> Any:
        return self.get_last_value()

    def get_borders(self) -> tuple:
        return self.get_first_item(), self.get_last_item()

    def get_mutual_borders(self, other: Native) -> list:
        assert isinstance(other, (SortedSeries, SortedSeriesInterface, SortedKeyValueSeriesInterface))
        first_item = max(self.get_first_item(), other.get_first_item())
        last_item = min(self.get_last_item(), other.get_last_item())
        if first_item < last_item:
            return [first_item, last_item]

    def borders(self, other: Native = None) -> Native:
        if other:
            result = self.new(self.get_mutual_borders(other))
        else:
            result = self.new(self.get_borders())
        return self._assume_native(result)

    @staticmethod
    def _assume_native(series) -> Native:
        return series


SeriesType.add_classes(AnySeries, SortedSeries)
