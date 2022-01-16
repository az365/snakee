from abc import ABC, abstractmethod
from typing import Optional, Callable, Iterable, Union

try:  # Assume we're a submodule in a package.
    from functions.primary import dates as dt
    from series.interpolation_type import InterpolationType
    from series.interfaces.any_series_interface import AnySeriesInterface, Name, NumericTypes
    from series.interfaces.date_series_interface import DateSeriesInterface
    from series.interfaces.numeric_series_interface import NumericSeriesInterface, OptNumeric
    from series.interfaces.key_value_series_interface import KeyValueSeriesInterface
    from series.interfaces.sorted_key_value_series_interface import SortedKeyValueSeriesInterface
    from series.interfaces.sorted_numeric_series_interface import SortedNumericSeriesInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...functions.primary import dates as dt
    from ..interpolation_type import InterpolationType
    from .any_series_interface import AnySeriesInterface, Name, NumericTypes
    from .date_series_interface import DateSeriesInterface
    from .numeric_series_interface import NumericSeriesInterface, OptNumeric
    from .key_value_series_interface import KeyValueSeriesInterface
    from .sorted_key_value_series_interface import SortedKeyValueSeriesInterface
    from .sorted_numeric_series_interface import SortedNumericSeriesInterface

Native = Union[SortedKeyValueSeriesInterface, NumericSeriesInterface]
DateNumeric = Union[Native, DateSeriesInterface]


class SortedNumericKeyValueSeriesInterface(SortedKeyValueSeriesInterface, NumericSeriesInterface, ABC):
    @abstractmethod
    def key_series(self, set_closure: bool = False, name: Name = None) -> SortedNumericSeriesInterface:
        pass

    @abstractmethod
    def value_series(self, set_closure: bool = False, name: Name = None) -> NumericSeriesInterface:
        pass

    @abstractmethod
    def get_numeric_keys(self) -> Iterable:
        pass

    @abstractmethod
    def assume_not_numeric(self, validate: bool = False, set_closure: bool = False) -> SortedKeyValueSeriesInterface:
        pass

    @abstractmethod
    def to_dates(
            self,
            as_iso_date: bool = False,
            scale: dt.DateScale = dt.DateScale.Day,
            set_closure: bool = False,
            inplace: bool = False,
    ) -> DateNumeric:
        pass

    @abstractmethod
    def get_range_len(self) -> int:
        pass

    @abstractmethod
    def distance(
            self,
            v: Union[Native, NumericTypes],
            take_abs: bool = True,
            inplace: bool = False,
    ) -> Union[Native, NumericTypes]:
        pass

    @abstractmethod
    def get_nearest_key(self, key: NumericTypes) -> NumericTypes:
        pass

    @abstractmethod
    def get_nearest_item(self, key: NumericTypes) -> tuple:
        pass

    @abstractmethod
    def get_two_nearest_keys(self, key: NumericTypes) -> Optional[tuple]:
        pass

    @abstractmethod
    def get_segment(self, key: NumericTypes) -> Native:
        pass

    @abstractmethod
    def derivative(self, extend: bool = False, default: NumericTypes = 0, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def get_spline_function(self, from_cache: bool = True, to_cache: bool = True) -> Callable:
        pass

    @abstractmethod
    def get_spline_interpolated_value(self, key: NumericTypes, default: OptNumeric = None) -> OptNumeric:
        pass

    @abstractmethod
    def get_linear_interpolated_value(self, key: NumericTypes, near_for_outside: bool = True) -> OptNumeric:
        pass

    @abstractmethod
    def get_interpolated_value(
            self,
            key: NumericTypes,
            how: InterpolationType = InterpolationType.Linear,
            *args, **kwargs
    ) -> NumericTypes:
        pass

    @abstractmethod
    def interpolate(
            self,
            keys: Iterable,
            how: InterpolationType = InterpolationType.Linear,
            *args, **kwargs
    ) -> Native:
        pass

    @abstractmethod
    def linear_interpolation(self, keys: Iterable, near_for_outside: bool = True) -> Native:
        pass

    @abstractmethod
    def spline_interpolation(self, keys: Iterable) -> Native:
        pass

    @abstractmethod
    def get_value_by_key(self, key: NumericTypes, *args, interpolate: str = 'linear', **kwargs) -> NumericTypes:
        pass
