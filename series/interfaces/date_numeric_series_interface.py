from abc import ABC, abstractmethod
from typing import Optional, Callable, Iterable, Union

try:  # Assume we're a submodule in a package.
    from utils.arguments import Auto, AUTO
    from functions.primary.dates import DateScale, DAYS_IN_WEEK, MAX_DAYS_IN_MONTH
    from series.interpolation_type import InterpolationType
    from series.interfaces.any_series_interface import AnySeriesInterface, Name
    from series.interfaces.date_series_interface import DateSeriesInterface, Date
    from series.interfaces.numeric_series_interface import NumericSeriesInterface, NumericValue
    from series.interfaces.sorted_numeric_series_interface import SortedNumericSeriesInterface
    from series.interfaces.sorted_numeric_key_value_series_interface import SortedNumericKeyValueSeriesInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.arguments import Auto, AUTO
    from ...functions.primary.dates import DateScale, DAYS_IN_WEEK, MAX_DAYS_IN_MONTH
    from ..interpolation_type import InterpolationType
    from .any_series_interface import AnySeriesInterface, Name
    from .date_series_interface import DateSeriesInterface, Date
    from .numeric_series_interface import NumericSeriesInterface, NumericValue
    from .sorted_numeric_series_interface import SortedNumericSeriesInterface
    from .sorted_numeric_key_value_series_interface import SortedNumericKeyValueSeriesInterface

Native = Union[SortedNumericKeyValueSeriesInterface, DateSeriesInterface]
Series = Union[Native, AnySeriesInterface]
AutoBool = Union[Auto, bool]
Window = Union[list, tuple]

WINDOW_WEEKLY_DEFAULT = -DAYS_IN_WEEK, 0, DAYS_IN_WEEK  # (-7, 0, 7)
DEFAULT_INTERPOLATION_KWARGS = ('how', InterpolationType.Linear),
DEFAULT_YOY_KWARGS = ('how', InterpolationType.Linear), ('near_for_outside', False)


class DateNumericSeriesInterface(SortedNumericKeyValueSeriesInterface, DateSeriesInterface, ABC):
    @abstractmethod
    def assume_numeric(self, validate: bool = False, set_closure: bool = False) -> Native:
        pass

    @abstractmethod
    def set_dates(self, dates: Iterable, inplace: bool = False, set_closure: bool = False) -> Native:
        pass

    @abstractmethod
    def filter_dates(self, function: Callable, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def numeric_key_series(self, inplace: bool = False) -> SortedNumericSeriesInterface:
        pass

    @abstractmethod
    def key_series(self, set_closure: bool = False, name: Name = None) -> DateSeriesInterface:
        pass

    @abstractmethod
    def value_series(self, set_closure: bool = False, name: Name = None) -> NumericSeriesInterface:
        pass

    @abstractmethod
    def round_to(self, scale: DateScale, as_iso_date: AutoBool = AUTO, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def get_segment(self, date: Date) -> Native:
        pass

    @abstractmethod
    def weighted_interpolation(
            self,
            dates: Iterable,
            weight_benchmark: Series,
            internal: InterpolationType = 'linear',
    ) -> Native:
        pass

    @abstractmethod
    def interpolate_to_scale(
            self,
            scale: DateScale,
            how: InterpolationType = InterpolationType.Spline,
            *args, **kwargs
    ) -> Series:
        pass

    @abstractmethod
    def apply_window_series_function(
            self,
            window_days_count: int,
            function: Callable,
            input_as_dict: bool = False,
            for_full_window_only: bool = False,
    ) -> Native:
        pass

    @abstractmethod
    def apply_interpolated_window_series_function(
            self,
            window_days_list: list,
            function: Callable,
            input_as_list: bool = False,
            for_full_window_only: bool = False,
    ) -> Native:
        pass

    @abstractmethod
    def smooth_linear_by_days(self, window_days_list: Window = WINDOW_WEEKLY_DEFAULT) -> Native:
        pass

    @abstractmethod
    def math(
            self,
            series: Series,
            function: Callable,
            interpolation_kwargs: Union[dict, tuple] = DEFAULT_INTERPOLATION_KWARGS,
    ) -> Native:
        pass

    @abstractmethod
    def yoy(
            self,
            interpolation_kwargs: Union[dict, tuple] = DEFAULT_YOY_KWARGS,
    ) -> Native:
        pass

    @abstractmethod
    def get_yoy_for_date(
            self,
            date: Date,
            interpolation_kwargs: Union[dict, tuple] = DEFAULT_INTERPOLATION_KWARGS,
    ) -> NumericValue:
        pass

    @abstractmethod
    def extrapolate_by_yoy(
            self,
            dates: Iterable,
            yoy: Optional[Series] = None,
            max_distance: int = MAX_DAYS_IN_MONTH,
            yoy_smooth_kwargs: Optional[dict] = None,
            interpolation_kwargs: Union[dict, tuple] = DEFAULT_INTERPOLATION_KWARGS,
    ) -> Native:
        pass

    def extrapolate(self, how: InterpolationType = InterpolationType.ByYoy, *args, **kwargs) -> Native:
        pass

    @abstractmethod
    def derivative(self, extend: bool = False, default: NumericValue = 0, inplace: bool = False) -> Native:
        pass
