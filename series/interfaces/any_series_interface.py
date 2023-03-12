from abc import ABC, abstractmethod
from typing import Optional, Callable, Iterable, Generator, Union, Any

try:  # Assume we're a submodule in a package.
    from base.interfaces.iterable_interface import IterableInterface
    from functions.primary.numeric import NUMERIC_TYPES, NumericTypes, DataFrame
    from series.series_type import SeriesType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.interfaces.iterable_interface import IterableInterface
    from ...functions.primary.numeric import NUMERIC_TYPES, NumericTypes, DataFrame
    from ..series_type import SeriesType

Native = IterableInterface
Series = IterableInterface


class AnySeriesInterface(IterableInterface, ABC):
    @abstractmethod
    def get_series_type(self) -> SeriesType:
        pass

    @abstractmethod
    def get_type(self) -> SeriesType:
        pass

    @abstractmethod
    def get_errors(self) -> Generator:
        pass

    @abstractmethod
    def value_series(self) -> Native:
        pass

    @abstractmethod
    def get_items(self) -> list:
        pass

    @abstractmethod
    def set_items(self, items: Iterable, inplace: bool, validate: bool = False, count: Optional[int] = None) -> Native:
        pass

    @abstractmethod
    def has_items(self) -> bool:
        pass

    @abstractmethod
    def get_count(self) -> int:
        pass

    @abstractmethod
    def get_range_numbers(self) -> Iterable:
        pass

    @abstractmethod
    def set_count(self, count: int, default: Any = None, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def drop_item_no(self, no: int, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def get_item_no(self, no: int, extend: bool = False, default: Any = None) -> Any:
        pass

    @abstractmethod
    def get_items_no(self, numbers: Iterable, extend: bool = False, default: Any = None) -> Generator:
        pass

    @abstractmethod
    def get_items_from_to(self, n_start: int, n_end: int) -> list:
        pass

    @abstractmethod
    def slice(self, n_start: int, n_end: int, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def crop(self, left_count: int, right_count: int, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def items_no(self, numbers: Iterable, extend: bool = False, default: Any = None, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def extend(self, series: Native, default: Any = None, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def intersect(self, series: Native, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def shift(self, distance: int, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def shift_values(self, diff: NumericTypes, inplace: bool = False):
        pass

    @abstractmethod
    def shift_value_positions(self, distance: int, default: Optional[Any] = None, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def append(self, value: Any, inplace: bool) -> Native:
        pass

    @abstractmethod
    def preface(self, value: Any, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def insert(self, pos: int, value: Any, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def add(
            self,
            obj_or_items: Union[Native, Iterable],
            before: bool = False,
            inplace: bool = False,
            **kwargs
    ) -> Native:
        pass

    @abstractmethod
    def filter(self, function: Callable, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def filter_values(self, function: Callable, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def filter_values_defined(self) -> Native:
        pass

    @abstractmethod
    def filter_values_nonzero(self) -> Native:
        pass

    @abstractmethod
    def condition_values(self, function: Callable) -> Native:
        pass

    @abstractmethod
    def map(self, function: Callable, inplace: bool = False, validate: bool = False) -> Native:
        pass

    @abstractmethod
    def map_values(self, function: Callable, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def map_zip_values(self, function: Callable, *series, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def map_extend_zip_values(self, function: Callable, inplace: bool = False, *series) -> Native:
        pass

    @abstractmethod
    def map_optionally_extend_zip_values(
            self,
            function: Callable,
            extend: bool,
            *series,
            inplace: bool = False,
    ) -> Native:
        pass

    @abstractmethod
    def apply(self, function: Callable, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def apply_to_values(self, function: Callable, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def assume_numeric(self, validate: bool = False) -> Series:
        pass

    @abstractmethod
    def to_numeric(self, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def assume_dates(self, validate: bool = False, set_closure: bool = False) -> Native:
        pass

    @abstractmethod
    def to_dates(self, as_iso_date: bool = False) -> Native:
        pass

    @abstractmethod
    def assume_unsorted(self) -> Native:
        pass

    @abstractmethod
    def assume_sorted(self) -> Native:
        pass

    @abstractmethod
    def sort(self, inplace: bool = False) -> Native:
        pass

    @abstractmethod
    def is_sorted(self, check: bool = True) -> bool:
        pass

    @abstractmethod
    def is_numeric(self, check: bool = False) -> bool:
        pass

    @staticmethod
    @abstractmethod
    def get_names() -> Union[list, tuple]:
        pass

    @abstractmethod
    def get_dataframe(self) -> DataFrame:
        pass
