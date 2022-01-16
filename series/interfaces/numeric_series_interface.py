from abc import ABC, abstractmethod
from typing import Optional, Callable, Iterable, Generator, Union, Any

try:  # Assume we're a submodule in a package.
    from functions.primary import dates as dt
    from series.series_type import SeriesType
    from series.interfaces.any_series_interface import AnySeriesInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...functions.primary import dates as dt
    from ..series_type import SeriesType
    from .any_series_interface import AnySeriesInterface

Native = AnySeriesInterface
Window = Union[list, tuple]
NumericValue = Union[int, float]
OptNumeric = Optional[NumericValue]

WINDOW_DEFAULT = -1, 0, 1
WINDOW_WO_CENTER = -2, -1, 0, 1, 2
WINDOW_NEIGHBORS = -1, 0


class NumericSeriesInterface(AnySeriesInterface, ABC):
    @staticmethod
    @abstractmethod
    def get_distance_func() -> Callable:
        pass

    @abstractmethod
    def has_valid_items(self) -> bool:
        pass

    @abstractmethod
    def get_sum(self) -> NumericValue:
        pass

    @abstractmethod
    def get_mean(self, default: OptNumeric = None) -> OptNumeric:
        pass

    @abstractmethod
    def norm(self, rate: OptNumeric = None, default: OptNumeric = None, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def divide(self, series: Native, default: OptNumeric = None, extend: bool = False, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def subtract(self, series: Native, default: Any = None, extend: bool = False, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def derivative(self, extend: bool = False, default: NumericValue = 0, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def get_sliding_window(
            self,
            window: Window = WINDOW_DEFAULT,
            extend: bool = True,
            default: bool = None,
            as_series: bool = True,
    ) -> Generator:
        pass

    @abstractmethod
    def apply_window_func(
            self,
            function: Callable,
            window: Window = WINDOW_DEFAULT,
            extend: bool = True,
            default: Any = None,
            as_series: bool = False,
            inplace: bool = False,
    ) -> Native:
        pass

    @abstractmethod
    def mark_local_extremes(self, local_min: bool = True, local_max: bool = True, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def mark_local_max(self, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def mark_local_min(self) -> Native:
        pass

    @abstractmethod
    def deviation_from_neighbors(
            self,
            window: Window = WINDOW_NEIGHBORS,
            relative: bool = False,
            inplace: bool = False,
    ) -> Native:
        pass

    @abstractmethod
    def smooth(self, how: str = 'linear', *args, **kwargs) -> Native:
        pass

    @abstractmethod
    def smooth_multiple(self, list_kwargs: Iterable = tuple()) -> Native:
        pass

    @abstractmethod
    def smooth_linear(self, window: Window = WINDOW_DEFAULT, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def smooth_spikes(
            self,
            threshold: NumericValue,
            window: Window = WINDOW_WO_CENTER,
            local_min: bool = False,
            local_max: bool = True,
            whitelist: Optional[Native] = None,
            inplace: bool = False,
    ) -> Native:
        pass

    @abstractmethod
    def mark_spikes(
            self,
            threshold: NumericValue,
            window: Window = WINDOW_NEIGHBORS,
            local_min: bool = False,
            local_max: bool = True,
            inplace: bool = False,
    ):
        pass

    @abstractmethod
    def plot(self, fmt: str = '-') -> None:
        pass
