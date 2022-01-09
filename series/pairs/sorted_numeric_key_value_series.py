from typing import Optional, Callable, Iterable, Generator, Union

try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from functions.primary import numeric as nm, dates as dt
    from series.series_type import SeriesType
    from series.interfaces.any_series_interface import AnySeriesInterface, Name, NumericTypes
    from series.interfaces.date_series_interface import DateSeriesInterface
    from series.interfaces.key_value_series_interface import KeyValueSeriesInterface
    from series.simple.sorted_numeric_series import SortedNumericSeries
    from series.pairs.sorted_key_value_series import SortedKeyValueSeries
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...functions.primary import numeric as nm, dates as dt
    from ..series_type import SeriesType
    from ..interfaces.any_series_interface import AnySeriesInterface, Name, NumericTypes
    from ..interfaces.date_series_interface import DateSeriesInterface
    from ..interfaces.key_value_series_interface import KeyValueSeriesInterface
    from ..simple.sorted_numeric_series import SortedNumericSeries
    from .sorted_key_value_series import SortedKeyValueSeries

Series = AnySeriesInterface
Sorted = Union[SortedKeyValueSeries, SortedNumericSeries]
Native = Sorted  # SortedNumericKeyValueSeriesInterface
NumericSeriesInterface = SortedNumericSeries  # NumericSeriesInterface
DateNumeric = Union[Native, Sorted, Series, DateSeriesInterface, KeyValueSeriesInterface]
OptNum = Optional[NumericTypes]

DYNAMIC_META_FIELDS = 'cached_spline',


class SortedNumericKeyValueSeries(SortedKeyValueSeries, SortedNumericSeries):
    def __init__(
            self,
            keys: Optional[Iterable] = None,
            values: Optional[Iterable] = None,
            set_closure: bool = False,
            validate: bool = False,
            sort_items: bool = True,
            name: Name = None,
    ):
        super().__init__(
            keys=keys,
            values=values,
            set_closure=set_closure,
            validate=validate,
            sort_items=sort_items,
            name=name,
        )
        self.cached_spline = None

    def get_series_type(self) -> SeriesType:
        return SeriesType.SortedNumericKeyValueSeries

    def get_errors(self) -> Generator:
        yield from super().get_errors()
        if not self.key_series().assume_numeric().has_valid_items():
            yield 'Keys of {} must be int of float'.format(self.get_class_name())
        if not self.value_series().has_valid_items():
            yield 'Values of {} must be int of float'.format(self.get_class_name())

    @staticmethod
    def get_distance_func() -> Callable:
        series_class = SeriesType.NumericSeries.get_class()
        return series_class.get_distance_func()

    @classmethod
    def _get_meta_member_names(cls) -> list:
        return list(super()._get_meta_member_names()) + ['cached_spline']

    @staticmethod
    def _get_dynamic_meta_fields() -> tuple:
        return DYNAMIC_META_FIELDS

    def set_meta(self, inplace: bool = False, **dict_meta) -> Native:
        if inplace:
            for k, v in dict_meta.items():
                if hasattr(v, 'copy') and k not in DYNAMIC_META_FIELDS:
                    v = v.copy()
                self.__dict__[k] = v
        else:
            return super().set_meta(**dict_meta, inplace=inplace)

    def key_series(self, set_closure: bool = False, name: Name = None) -> SortedNumericSeries:
        series_class = SeriesType.SortedNumericSeries.get_class()
        return series_class(self.get_keys(), set_closure=set_closure, validate=False, name=name)

    def value_series(self, set_closure: bool = False, name: Name = None) -> NumericSeriesInterface:
        series_class = SeriesType.NumericSeries.get_class()
        return series_class(self.get_values(), set_closure=set_closure, validate=False, name=name)

    def get_numeric_keys(self) -> Iterable:
        return self.get_keys()

    def assume_numeric(self, validate: bool = False, set_closure: bool = False) -> Native:
        return self.validate() if validate else self

    def assume_not_numeric(self, validate: bool = False, set_closure: bool = False) -> SortedKeyValueSeries:
        series_class = SeriesType.SortedKeyValueSeries.get_class()
        return series_class(**self._get_data_member_dict(), validate=validate, set_closure=set_closure)

    def to_dates(
            self,
            as_iso_date: bool = False,
            scale: dt.DateScale = dt.DateScale.Day,
            set_closure: bool = False,
            inplace: bool = False,
    ) -> DateNumeric:
        result = self.map_keys(
            function=lambda d: dt.get_date_from_int(d, scale=scale, as_iso_date=as_iso_date),
            sorting_changed=False,
            inplace=inplace,
        ) or self
        result = result.assume_dates(set_closure=set_closure, validate=False)
        return self._assume_native(result)

    def get_range_len(self) -> int:
        return self.get_distance_func()(
            *self.key_series().get_borders()
        )

    def distance(
            self,
            v: Union[Native, NumericTypes],
            take_abs: bool = True,
            inplace: bool = False,
    ) -> Union[Native, NumericTypes]:
        return self.key_series(set_closure=True).distance(v, take_abs, inplace=inplace) or self

    def get_nearest_key(self, key: NumericTypes) -> NumericTypes:
        distance_func = self.get_distance_func()
        return self.key_series(set_closure=True).get_nearest_value(key, distance_func=distance_func)

    def get_nearest_item(self, key: NumericTypes) -> tuple:
        nearest_key = self.get_nearest_key(key)
        return nearest_key, self.get_value_by_key(nearest_key)

    def get_two_nearest_keys(self, key: NumericTypes) -> Optional[tuple]:
        if self.get_count() < 2:
            return None
        else:
            distance_series = self.distance(key, take_abs=False)
            date_a = distance_series.filter_values(lambda v: v < 0).get_arg_max()
            date_b = distance_series.filter_values(lambda v: v >= 0).get_arg_min()
            return date_a, date_b

    def get_segment(self, key: NumericTypes) -> Native:
        nearest_keys = [i for i in self.get_two_nearest_keys(key) if i]
        return self.new().from_items(
            [(d, self.get_value_by_key(d)) for d in nearest_keys],
        )

    def derivative(self, extend: bool = False, default: NumericTypes = 0, inplace: bool = False) -> Native:
        dx = self.key_series(set_closure=True).derivative(extend=extend, default=default)  # not inplace, key used later
        dy = self.value_series(set_closure=True).derivative(extend=extend, default=default, inplace=inplace)
        derivative = dy.divide(dx, default=default, inplace=inplace) or dy
        keys = self.get_numeric_keys()
        values = derivative.get_values()
        if inplace:
            derivative.set_keys(keys, inplace=True, set_closure=True, sort_items=False)
            derivative.set_values(values, inplace=True, set_closure=True)
            return derivative
        else:
            result = self.new(keys=keys, values=values, sort_items=False, validate=False, save_meta=True)
            return self._assume_native(result)

    def get_spline_function(self, from_cache: bool = True, to_cache: bool = True) -> Callable:
        if from_cache and self.cached_spline:
            spline_function = self.cached_spline
        else:
            spline_function = nm.spline_interpolate(
                self.get_numeric_keys(),
                self.get_values(),
            )
            if to_cache:
                self.cached_spline = spline_function
        return spline_function

    def get_spline_interpolated_value(self, key: NumericTypes, default: OptNum = None) -> OptNum:
        if self.has_key_in_range(key):
            spline_function = self.get_spline_function(from_cache=True, to_cache=True)
            return float(spline_function(key))
        else:
            return default

    def get_linear_interpolated_value(self, key: NumericTypes, near_for_outside: bool = True) -> OptNum:
        segment = self.get_segment(key)
        if segment.get_count() == 1:
            if near_for_outside:
                return segment.get_first_value()
        elif segment.get_count() == 2:
            [(key_a, value_a), (key_b, value_b)] = segment.get_list()
            segment_days = segment.get_range_len()
            distance_days = self.get_distance_func()(key_a, key)
            interpolated_value = value_a + (value_b - value_a) * distance_days / segment_days
            return interpolated_value

    def get_interpolated_value(self, key: NumericTypes, how: str = 'linear', *args, **kwargs) -> NumericTypes:
        method_name = 'get_{}_interpolated_value'.format(how)
        interpolation_method = self.__getattribute__(method_name)
        return interpolation_method(key, *args, **kwargs)

    def interpolate(self, keys: Iterable, how: str = 'linear', *args, **kwargs) -> Native:
        method_name = '{}_interpolation'.format(how)
        interpolation_method = self.__getattribute__(method_name)
        return interpolation_method(keys, *args, **kwargs)

    def linear_interpolation(self, keys: Iterable, near_for_outside: bool = True) -> Native:
        result = self.new(save_meta=True)
        for k in keys:
            result.append_pair(k, self.get_linear_interpolated_value(k, near_for_outside), inplace=True)
        return self._assume_native(result)

    def spline_interpolation(self, keys: Iterable) -> Native:
        spline_function = self.get_spline_function(from_cache=True, to_cache=True)
        result = self.new(
            keys=keys,
            values=spline_function(list(keys)),
            save_meta=True,
        )
        return self._assume_native(result)

    def get_value_by_key(self, key: NumericTypes, *args, interpolate: str = 'linear', **kwargs) -> NumericTypes:
        value = self.get_dict().get(key)
        if value is None:
            value = self.get_interpolated_value(key, how=interpolate, *args, **kwargs)
        return value

    @staticmethod
    def _assume_native(series) -> Native:
        return series


SeriesType.add_classes(SortedNumericSeries, SortedKeyValueSeries, SortedNumericKeyValueSeries)
