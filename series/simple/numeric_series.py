from typing import Optional, Callable, Iterable, Generator, Any

try:  # Assume we're a submodule in a package.
    from functions.primary.numeric import plot
    from functions.secondary import numeric_functions as fs
    from series.series_type import SeriesType
    from series.interfaces.numeric_series_interface import (
        NumericSeriesInterface,
        NumericValue, OptNumeric, Window,
        WINDOW_DEFAULT, WINDOW_WO_CENTER, WINDOW_NEIGHBORS,
    )
    from series.simple.any_series import AnySeries
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...functions.primary.numeric import plot
    from ...functions.secondary import numeric_functions as fs
    from ..series_type import SeriesType
    from ..interfaces.numeric_series_interface import (
        NumericSeriesInterface,
        NumericValue, OptNumeric, Window,
        WINDOW_DEFAULT, WINDOW_WO_CENTER, WINDOW_NEIGHBORS,
    )
    from .any_series import AnySeries

Native = NumericSeriesInterface

DEFAULT_NUMERIC = True


class NumericSeries(AnySeries, NumericSeriesInterface):
    def __init__(
            self,
            values: Optional[Iterable] = None,
            set_closure: bool = False,
            validate: bool = False,
            name: Optional = None,
    ):
        super().__init__(values=values, set_closure=set_closure, validate=validate, name=name)

    def get_series_type(self) -> SeriesType:
        return SeriesType.NumericSeries

    @staticmethod
    def get_distance_func(constant=None, take_abs: bool = False, default=None) -> Callable:
        return fs.increment(constant, take_abs=take_abs, default=default)

    def get_errors(self) -> Generator:
        yield from super().get_errors()
        if not self.has_valid_items():
            yield 'Values of {} must be numeric'.format(self.get_class_name())

    def has_valid_items(self) -> bool:
        for v in self.get_values():
            if not isinstance(v, (int, float)):
                return False
        return True

    def is_numeric(self, check: bool = False) -> bool:
        if check:
            return self.has_valid_items()
        else:
            return DEFAULT_NUMERIC

    def get_sum(self) -> NumericValue:
        values = self.filter_values_defined().get_values()
        return sum(values)

    def get_mean(self, default: OptNumeric = None) -> OptNumeric:
        values_defined = self.filter_values_defined().get_values()
        if values_defined:
            return sum(values_defined) / len(values_defined)
        else:
            return default

    def norm(self, rate: OptNumeric = None, default: OptNumeric = None, inplace: bool = False) -> Native:
        if rate is None:
            rate = self.get_mean()
        return self.map_values(lambda v: v / rate if rate else default, inplace=inplace) or self

    def divide(self, series: Native, default: OptNumeric = None, extend: bool = False, inplace: bool = False) -> Native:
        result = self.map_optionally_extend_zip_values(
            fs.div(default=default),
            series,
            extend=extend,
            inplace=inplace,
        )
        return self._assume_native(result)

    def subtract(self, series: Native, default: Any = None, extend: bool = False, inplace: bool = False) -> Native:
        result = self.map_optionally_extend_zip_values(
            fs.diff(default=default),
            series,
            extend=extend,
            inplace=inplace,
        )
        return self._assume_native(result)

    def derivative(self, extend: bool = False, default: NumericValue = 0, inplace: bool = False) -> Native:
        if extend:
            return self.preface(
                None, inplace=inplace,
            ).subtract(
                self,
                extend=True,
                default=default,
                inplace=inplace,
            ).crop(
                0, 1, inplace=inplace,
            ) or self
        else:
            return self.slice(
                0, -1, inplace=inplace,
            ).subtract(
                self.shift(-1),
                inplace=inplace,
            )

    def get_sliding_window(
            self,
            window: Window = WINDOW_DEFAULT,
            extend: bool = True,
            default: bool = None,
            as_series: bool = True,
    ) -> Generator:
        if extend:
            n_min = 0
            n_max = self.get_count()
        else:
            n_min = - min(window)
            n_max = self.get_count() - max(window)
        for center in range(n_min, n_max):
            sliding_window = [center + n for n in window]
            if as_series:
                yield self.value_series().items_no(sliding_window, extend=extend, default=default)
            else:
                yield self.value_series().get_items_no(sliding_window, extend=extend, default=default)

    def apply_window_func(
            self,
            function: Callable,
            window: Window = WINDOW_DEFAULT,
            extend: bool = True,
            default: Any = None,
            as_series: bool = False,
            inplace: bool = False,
    ) -> Native:
        values = map(function, self.get_sliding_window(window, extend=extend, default=default, as_series=as_series))
        result = self.set_values(values, inplace=inplace)
        return self._assume_native(result) or self

    def mark_local_extremes(self, local_min: bool = True, local_max: bool = True, inplace: bool = False) -> Native:
        win_len = len(WINDOW_DEFAULT)
        assert win_len == 3, 'is_local_extreme() function accept only 3 numeric arguments, got {}'.format(win_len)
        return self.apply_window_func(
            fs.is_local_extreme(local_min=local_min, local_max=local_max),
            window=WINDOW_DEFAULT,
            extend=True,
            default=False,
            inplace=inplace,
        )

    def mark_local_max(self, inplace: bool = False) -> Native:
        return self.mark_local_extremes(local_min=False, local_max=True, inplace=inplace)

    def mark_local_min(self) -> Native:
        return self.mark_local_extremes(local_min=True, local_max=False)

    def deviation_from_neighbors(
            self,
            window: Window = WINDOW_NEIGHBORS,
            relative: bool = False,
            inplace: bool = False,
    ) -> Native:
        smoothed_series = self.smooth(window=window, inplace=False)
        deviation = self.subtract(smoothed_series, inplace=inplace) or self
        if relative:
            deviation = deviation.divide(smoothed_series, default=0, inplace=inplace)
        return deviation or self

    # @deprecated
    def smooth_simple_linear(self, window_len: int = 3, exclude_center: bool = False) -> Native:
        center = int((window_len - 1) / 2)
        count = self.get_count()
        result = self._assume_native(self.new())
        for n in self.get_range_numbers():
            is_edge = n < center or n >= count - center
            if is_edge:
                result.append(self.get_item_no(n), inplace=True)
            else:
                sub_series = self.slice(n - center, n + center + 1, inplace=False)
                if exclude_center:
                    sub_series = sub_series.drop_item_no(center)
                result.append(sub_series.get_mean(), inplace=True)
        return self._assume_native(result)

    def smooth(self, how: str = 'linear', *args, **kwargs) -> Native:
        method_name = 'smooth_{}'.format(how)
        smooth_method = self.__getattribute__(method_name)
        return smooth_method(*args, **kwargs)

    def smooth_multiple(self, list_kwargs: Iterable = tuple()) -> Native:
        series = self
        for kwargs in list_kwargs:
            series = series.smooth(**kwargs)
        return series

    def smooth_linear(self, window: Window = WINDOW_DEFAULT, inplace: bool = False) -> Native:
        return self.apply_window_func(
            lambda s: s.get_mean(),
            window=window, extend=True, default=None,
            as_series=True, inplace=inplace,
        )

    def smooth_spikes(
            self,
            threshold: NumericValue,
            window: Window = WINDOW_WO_CENTER,
            local_min: bool = False,
            local_max: bool = True,
            whitelist: Optional[Native] = None,
            inplace: bool = False,
    ) -> Native:
        spikes = self.mark_spikes(threshold, local_min=local_min, local_max=local_max, inplace=inplace) or self
        if whitelist:
            spikes = spikes.map_zip_values(
                lambda a, b: a and not b,
                whitelist,
                inplace=inplace,
            ) or spikes
        series = self.map_zip_values(
            lambda v, t, s: s if t else v,
            spikes,
            self.smooth(window=window, inplace=False),
            inplace=inplace,
        ) or self
        return self._assume_native(series)

    def mark_spikes(
            self,
            threshold: NumericValue,
            window: Window = WINDOW_NEIGHBORS,
            local_min: bool = False,
            local_max: bool = True,
            inplace: bool = False,
    ):
        deviation = self.deviation_from_neighbors(window=window, relative=True, inplace=False)
        if local_min or local_max:
            deviation = deviation.map_zip_values(
                lambda x, m: x if m else None,
                self.mark_local_extremes(local_min=local_min, local_max=local_max),
                inplace=inplace,
            ) or deviation
        spikes = deviation.map_values(
            lambda x: abs(x or 0) > threshold,
            inplace=inplace,
        ) or deviation
        return spikes

    def plot(self, fmt: str = '-') -> None:
        plot(self.get_range_numbers(), self.get_values(), fmt=fmt)

    @staticmethod
    def _assume_native(series) -> Native:
        return series


SeriesType.add_classes(AnySeries, NumericSeries)
