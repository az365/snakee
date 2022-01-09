from typing import Optional, Callable, Iterable, Generator, Union, NoReturn

try:  # Assume we're a submodule in a package.
    from functions.primary import numeric as nm, dates as dt
    from series.series_type import SeriesType
    from series.interfaces.any_series_interface import AnySeriesInterface, Name, NumericTypes
    from series.interfaces.key_value_series_interface import KeyValueSeriesInterface
    from series.simple.date_series import DateSeries
    from series.pairs.sorted_numeric_key_value_series import SortedNumericKeyValueSeries
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...functions.primary import numeric as nm, dates as dt
    from ..series_type import SeriesType
    from ..interfaces.any_series_interface import AnySeriesInterface, Name, NumericTypes
    from ..interfaces.key_value_series_interface import KeyValueSeriesInterface
    from ..simple.date_series import DateSeries
    from .sorted_numeric_key_value_series import SortedNumericKeyValueSeries

Native = Union[DateSeries, SortedNumericKeyValueSeries]
DateNumericSeriesInterface = KeyValueSeriesInterface  # tmp
DateNumeric = DateNumericSeriesInterface
Series = Union[Native, AnySeriesInterface, KeyValueSeriesInterface, DateNumeric]
Window = Union[list, tuple]
Date = dt.Date
InterpolationType = str

DYNAMIC_META_FIELDS = 'cached_yoy', 'cached_spline'
WINDOW_WEEKLY_DEFAULT = -dt.DAYS_IN_WEEK, 0, dt.DAYS_IN_WEEK  # (-7, 0, 7)


class DateNumericSeries(SortedNumericKeyValueSeries, DateSeries):
    def __init__(
            self,
            keys: Optional[list] = None,
            values: Optional[list] = None,
            set_closure: bool = False,
            validate: bool = False,
            sort_items: bool = True,
            name: Name = None,
    ):
        self.cached_yoy = None
        super().__init__(
            keys=keys,
            values=values,
            set_closure=set_closure,
            sort_items=sort_items,
            validate=validate,
            name=name,
        )

    @staticmethod
    def _get_dynamic_meta_fields() -> tuple:
        return DYNAMIC_META_FIELDS

    def get_errors(self) -> Generator:
        yield from self.assume_not_numeric().get_errors()
        if not self.date_series().is_valid():
            yield 'Keys of {} must be sorted dates'.format(self.get_class_name())
        if not self.value_series().is_valid():
            yield 'Values of {} must be numeric'.format(self.get_class_name())

    @staticmethod
    def get_distance_func() -> Callable:
        return DateSeries.get_distance_func()

    def assume_numeric(self, validate: bool = False, set_closure: bool = False) -> Series:
        series_class = SeriesType.SortedNumericKeyValueSeries.get_class()
        return series_class(**self._get_data_member_dict(), validate=validate, set_closure=set_closure)

    def get_dates(self, as_date_type: Optional[bool] = None) -> list:
        if as_date_type:
            return self.date_series().map(dt.to_date).get_values()
        else:
            return self.get_keys()

    def set_dates(self, dates: Iterable, inplace: bool = False, set_closure: bool = False) -> Series:
        if inplace:
            result = self.set_keys(dates, inplace=True, set_closure=set_closure) or self
        else:
            result = self.new(
                keys=dates,
                values=self.get_values(),
                set_closure=set_closure,
                save_meta=True,
                sort_items=False,
                validate=False,
            )
        return self._assume_native(result)

    def filter_dates(self, function: Callable, inplace: bool = False) -> Series:
        return self.filter_keys(function, inplace=inplace) or self

    def get_numeric_keys(self, inplace: bool = False) -> list:
        sorted_numeric_series = self.to_days(inplace=inplace) or self
        return sorted_numeric_series.get_keys()

    def numeric_key_series(self, inplace: bool = False) -> Series:
        sorted_numeric_series = self.to_days(inplace=inplace) or self
        return sorted_numeric_series.key_series(set_closure=True)

    def key_series(self, set_closure: bool = False, name: Name = None) -> Series:
        series = self.date_series(set_closure=set_closure)
        return self._assume_native(series)

    def value_series(self, set_closure: bool = False, name: Name = None) -> Series:
        series = super().value_series(set_closure=set_closure, name=name).assume_numeric(validate=False)
        return self._assume_native(series)

    def round_to_weeks(self, inplace: bool = False) -> Native:
        series = self.map_dates(dt.get_monday_date, inplace=inplace) or self
        return series.mean_by_keys()

    def round_to_months(self, inplace: bool = False) -> Native:
        series = self.map_dates(dt.get_month_first_date, inplace=inplace) or self
        return series.mean_by_keys()

    def get_segment(self, date: Date) -> Series:
        nearest_dates = [i for i in self.get_two_nearest_dates(date) if i]
        items = [(d, self.get_value_by_key(d)) for d in nearest_dates]
        return self.new().from_items(items)

    def get_nearest_value(self, value: NumericTypes, distance_func: Optional[Callable] = None) -> NumericTypes:
        return self.value_series().sort().get_nearest_value(value, distance_func)

    def get_interpolated_value(self, date: Date, how: InterpolationType = 'linear', *args, **kwargs) -> NumericTypes:
        method_name = 'get_{}_interpolated_value'.format(how)
        requires_numeric_keys = how in ('linear', 'spline')
        if requires_numeric_keys:
            numeric_series = self.to_days()
            interpolation_method = numeric_series.__getattribute__(method_name)
            numeric_key = dt.get_day_abs_from_date(date)
            return interpolation_method(numeric_key, *args, **kwargs)
        else:
            interpolation_method = self.__getattribute__(method_name)
            return interpolation_method(date, *args, **kwargs)

    def interpolate(self, dates: Iterable, how: InterpolationType = 'linear', *args, **kwargs) -> Series:
        method_name = '{}_interpolation'.format(how)
        requires_numeric_keys = how in ('linear', 'spline')
        if requires_numeric_keys:
            interpolation_method = self.to_days().__getattribute__(method_name)
            series_class = SeriesType.DateSeries.get_class()
            numeric_keys = series_class(dates, sort_items=True).to_days()
            return interpolation_method(numeric_keys, *args, **kwargs).to_dates()
        else:
            interpolation_method = self.__getattribute__(method_name)
            return interpolation_method(dates, *args, **kwargs)

    def weighted_interpolation(
            self,
            dates: Iterable,
            weight_benchmark: Series,
            internal: InterpolationType = 'linear',
    ) -> Series:
        assert isinstance(weight_benchmark, (DateNumericSeries, DateNumeric)), 'got {}'.format(weight_benchmark)
        list_dates = dates.get_dates() if isinstance(dates, (DateNumericSeries, DateNumericSeriesInterface)) else dates
        border_dates = self.get_mutual_border_dates(weight_benchmark)
        result = self.new(save_meta=True)
        for d in list_dates:
            yearly_dates = dt.get_yearly_dates(d, *border_dates)
            if yearly_dates:
                yearly_primary = self.interpolate(yearly_dates, how=internal)
                yearly_benchmark = weight_benchmark.interpolate(yearly_dates, how=internal)
                norm_benchmark = yearly_benchmark.divide(yearly_primary)
                weight = norm_benchmark.get_mean()
                pre_interpolated_value = self.get_interpolated_value(d, how=internal)
                weighted_value = pre_interpolated_value * weight
                result.append_pair(d, weighted_value, inplace=True)
        return self._assume_series(result)

    def interpolate_to_weeks(self, how: InterpolationType = 'spline', *args, **kwargs) -> Series:
        monday_dates = dt.get_weeks_range(self.get_first_date(), self.get_last_date())
        return self.interpolate(monday_dates, how=how, *args, **kwargs)

    def interpolate_to_months(self, how: InterpolationType = 'spline', *args, **kwargs) -> Series:
        monthly_dates = dt.get_months_range(self.get_first_date(), self.get_last_date())
        return self.interpolate(monthly_dates, how=how, *args, **kwargs)

    def apply_window_series_function(
            self,
            window_days_count: int,
            function: Callable,
            input_as_dict: bool = False,
            for_full_window_only: bool = False,
    ) -> Series:
        half_window_days = window_days_count / 2
        int_half_window_days = int(half_window_days)
        window_days_is_even = half_window_days == int_half_window_days
        left_days = int_half_window_days
        right_days = int_half_window_days if window_days_is_even else int_half_window_days + 1
        result = self.new(save_meta=True)
        if for_full_window_only:
            dates = self.crop(left_days, right_days).get_dates()
        else:
            dates = self.get_dates()
        for center_date in dates:
            window = self.span(
                dt.get_shifted_date(center_date, -left_days),
                dt.get_shifted_date(center_date, right_days),
            )
            if input_as_dict:
                window = window.get_dict()
            result.append_pair(center_date, function(window), inplace=True)
        return self._assume_native(result)

    def apply_interpolated_window_series_function(
            self,
            window_days_list: list,
            function: Callable,
            input_as_list: bool = False,
            for_full_window_only: bool = False,
    ):
        result = self.new(save_meta=True)
        left_days = min(window_days_list)
        right_days = max(window_days_list)
        if for_full_window_only:
            dates = self.crop(left_days, right_days).get_dates()
        else:
            dates = self.get_dates()
        for d in dates:
            window_dates = [dt.get_shifted_date(d, days) for days in window_days_list]
            window = self.interpolate(window_dates)
            window_values = window.get_values()
            if None not in window_values or not for_full_window_only:
                if input_as_list:
                    window = window_values
                result.append_pair(d, function(window), inplace=True)
        return result

    def smooth_linear_by_days(self, window_days_list: Window = WINDOW_WEEKLY_DEFAULT) -> Native:
        result = self.apply_interpolated_window_series_function(
            window_days_list=window_days_list,
            function=lambda s: s.get_mean(),
            input_as_list=False,
            for_full_window_only=False,
        )
        return self._assume_native(result)

    def math(self, series: Series, function: Callable, interpolation_kwargs: dict = {'how': 'linear'}) -> Series:
        assert isinstance(series, (DateNumericSeries, DateNumericSeriesInterface)), 'got {}'.format(series)
        result = self.new(save_meta=True)
        for d, v in self.get_items():
            if v is not None:
                v0 = series.get_interpolated_value(d, **interpolation_kwargs)
                if v0 is not None:
                    result.append_pair(d, function(v, v0), inplace=True)
        return result

    def yoy(self, interpolation_kwargs: dict = {'how': 'linear', 'near_for_outside': False}) -> Series:
        yearly_shifted = self.yearly_shift()
        return self.math(
            yearly_shifted,
            function=lambda a, b: (a - b) / b if b else None,
            interpolation_kwargs=interpolation_kwargs,
        )

    def get_yoy_for_date(self, date: Date, interpolation_kwargs: dict = {'how': 'linear'}) -> NumericTypes:
        if not self.cached_yoy:
            self.cached_yoy = self.yoy(interpolation_kwargs=interpolation_kwargs)
        if date in self.cached_yoy:
            return self.get_value_by_key(date)
        elif date < self.get_first_date():
            return self.first_year().get_mean()
        elif date > self.get_last_date():
            return self.last_year().get_mean()
        else:
            return self.get_interpolated_value(date, **interpolation_kwargs)

    def extrapolate_by_yoy(
            self,
            dates: Iterable,
            yoy: Optional[Series] = None,
            max_distance: int = dt.MAX_DAYS_IN_MONTH,
            yoy_smooth_kwargs: Optional[dict] = None,
            interpolation_kwargs: dict = {'how': 'linear'},
    ) -> Series:
        if not yoy:
            yoy = self.yoy()
        if yoy_smooth_kwargs is not None:
            yoy = yoy.smooth(**yoy_smooth_kwargs)
        result = self.new()
        for d in dates:
            base_date, increment = self.find_base_date(d, max_distance=max_distance, return_increment=True)
            if not increment:
                cur_value = self.get_interpolated_value(d, **interpolation_kwargs)
            else:
                yoy_date = yoy.find_base_date(d, max_distance=max_distance, return_increment=False)
                if yoy_date:
                    cur_yoy = yoy.get_interpolated_value(yoy_date, **interpolation_kwargs)
                elif increment > 0:
                    cur_yoy = yoy.last_year().value_series().get_mean()
                else:  # increment < 0
                    cur_yoy = yoy.first_year().value_series().get_mean()
                base_value = self.get_interpolated_value(base_date)
                coefficient = (1 + cur_yoy) ** increment
                cur_value = base_value * coefficient
            result.append_pair(d, cur_value, inplace=True)
        return self._assume_native(result)

    def extrapolate_by_stl(self, dates: Iterable) -> NoReturn:
        raise NotImplemented

    def extrapolate(self, how: InterpolationType = 'by_yoy', *args, **kwargs) -> Series:
        method_name = 'extrapolate_{}'.format(how)
        extrapolation_method = self.__getattribute__(method_name)
        return extrapolation_method(*args, **kwargs)

    def derivative(self, extend: bool = False, default: NumericTypes = 0, inplace: bool = False) -> Series:
        derivative = self.value_series(set_closure=True).derivative(extend=extend, default=default)
        result = self.set_values(derivative.get_values(), inplace=inplace, set_closure=True, validate=False)
        return self._assume_series(result)

    @staticmethod
    def get_names() -> tuple:
        return 'date', 'value'

    def plot(self, fmt: str = '-') -> None:
        nm.plot(self.get_keys(), self.get_values(), fmt=fmt)

    @staticmethod
    def _assume_series(series) -> Series:
        return series

    @staticmethod
    def _assume_native(series) -> Native:
        return series


SeriesType.add_classes(DateSeries, DateNumericSeries, SortedNumericKeyValueSeries)
