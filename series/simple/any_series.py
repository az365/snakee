from typing import Optional, Callable, Iterable, Generator, Sized, Union, Any

try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from functions.primary import numeric as nm
    from series.interfaces.any_series_interface import AnySeriesInterface
    from series.interfaces.sorted_series_interface import SortedSeriesInterface
    from series.interfaces.date_series_interface import DateSeriesInterface
    from series.interfaces.numeric_series_interface import NumericSeriesInterface
    from series.interfaces.key_value_series_interface import KeyValueSeriesInterface
    from series.abstract_series import AbstractSeries
    from series.series_type import SeriesType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...functions.primary import numeric as nm
    from ..interfaces.any_series_interface import AnySeriesInterface
    from ..interfaces.sorted_series_interface import SortedSeriesInterface
    from ..interfaces.date_series_interface import DateSeriesInterface
    from ..interfaces.numeric_series_interface import NumericSeriesInterface
    from ..interfaces.key_value_series_interface import KeyValueSeriesInterface
    from ..abstract_series import AbstractSeries
    from ..series_type import SeriesType

Native = AnySeriesInterface

DEFAULT_NUMERIC = False
DEFAULT_SORTED = False


class AnySeries(AbstractSeries, AnySeriesInterface):
    def __init__(
            self,
            values: Iterable,
            set_closure: bool = False,
            validate: bool = False,
            name: Optional[str] = None,
    ):
        super().__init__(values=values, set_closure=set_closure, validate=validate, name=name)

    def get_series_type(self) -> SeriesType:
        return SeriesType.AnySeries

    @classmethod
    def _get_meta_member_names(cls) -> list:
        return list()

    def get_errors(self) -> Generator:
        values = self.get_values()
        if not isinstance(values, list):
            yield 'Values must be a list, not {}'.format(type(values))

    def value_series(self) -> Native:
        return self

    def get_items(self) -> list:
        return self.get_values()

    def set_items(self, items: Iterable, inplace: bool, validate: bool = False, count: Optional[int] = None) -> Native:
        if arg.is_defined(count) and isinstance(items, Sized):
            assert count == len(items), '{} != len({})'.format(count, items)
        return self.set_values(items, inplace=inplace, validate=validate) or self

    def has_items(self) -> bool:
        return bool(self.get_values())

    def get_count(self) -> int:
        return len(self.get_values())

    def get_range_numbers(self) -> Iterable:
        return range(self.get_count())

    def set_count(self, count: int, default: Any = None, inplace: bool = False) -> Native:
        if count > self.get_count():
            additional_values = [default] * (self.get_count() - count)
            return self.add(additional_values, inplace=inplace)
        else:
            return self.slice(0, count, inplace=inplace)

    def drop_item_no(self, no: int, inplace: bool = False) -> Native:
        if inplace:
            if no < self.get_count():
                self.get_values().pop(no)
            return self
        else:
            series = self.slice(0, no).add(
                self.slice(no + 1, self.get_count()),
            )
            return self._assume_native(series)

    def get_item_no(self, no: int, extend: bool = False, default: Any = None) -> Any:
        if extend:
            if no < self.get_count():
                return self.get_list()[no]
            else:
                return default
        else:
            return self.get_list()[no]

    def get_items_no(self, numbers: Iterable, extend: bool = False, default: Any = None) -> Generator:
        for no in numbers:
            yield self.get_item_no(no, extend=extend, default=default)

    def get_items_from_to(self, n_start: int, n_end: int) -> list:
        return self.get_list()[n_start: n_end]

    def slice(self, n_start: int, n_end: int, inplace: bool = False) -> Native:
        items = self.get_items_from_to(n_start, n_end)
        return self.set_items(items, inplace=inplace)

    def crop(self, left_count: int, right_count: int, inplace: bool = False) -> Native:
        return self.slice(
            n_start=left_count,
            n_end=self.get_count() - right_count,
            inplace=inplace,
        )

    def items_no(self, numbers: Iterable, extend: bool = False, default: Any = None, inplace: bool = False) -> Native:
        items = self.get_items_no(numbers, extend=extend, default=default)
        return self.set_items(items, inplace=inplace)

    def extend(self, series: Native, default: Any = None, inplace: bool = False) -> Native:
        count = series.get_count()
        if self.get_count() < count:
            return self.set_count(count, default, inplace=inplace)
        else:
            return self

    def intersect(self, series: Native, inplace: bool = False) -> Native:
        count = series.get_count()
        if self.get_count() > count:
            return self.set_count(count, inplace=inplace)
        else:
            return self

    def shift(self, distance: int, inplace: bool = False) -> Native:
        return self.shift_value_positions(distance, inplace=inplace)

    def shift_values(self, diff: nm.NumericTypes, inplace: bool = False):
        assert isinstance(diff, nm.NUMERIC_TYPES)
        return self.map_values(lambda v: v + diff, inplace=inplace) or self

    def shift_value_positions(self, distance: int, default: Optional[Any] = None, inplace: bool = False) -> Native:
        if distance > 0:
            values = [default] * distance + self.get_values()
            result = self.set_values(values, inplace=inplace)
        else:
            result = self.slice(n_start=-distance, n_end=self.get_count(), inplace=inplace) or self
        return self._assume_native(result)

    def append(self, value: Any, inplace: bool) -> Native:
        if inplace:
            self.get_values().append(value)
            return self
        else:
            new = self._assume_native(self.copy())
            new.append(value, inplace=True)
            return self._assume_native(new)

    def preface(self, value: Any, inplace: bool = False) -> Native:
        return self.insert(
            pos=0,
            value=value,
            inplace=inplace,
        ) or self

    def insert(self, pos: int, value: Any, inplace: bool = False) -> Native:
        if inplace:
            self.get_values().insert(pos, value)
        else:
            new = self._assume_native(self.copy())
            new.insert(pos, value, inplace=True)
            return self._assume_native(new)

    def add(
            self,
            obj_or_items: Union[Native, Iterable],
            before: bool = False,
            inplace: bool = False,
            **kwargs
    ) -> Native:
        if hasattr(obj_or_items, 'get_values'):
            added_values = obj_or_items.get_values()
        else:
            added_values = obj_or_items
        result = super().add(added_values, before=before, inplace=inplace, **kwargs)
        return self._assume_native(result) or self

    def filter(self, function: Callable, inplace: bool = False) -> Native:
        filtered_items = filter(function, self.get_items())
        return self.set_items(filtered_items, inplace=inplace) or self

    def filter_values(self, function: Callable, inplace: bool = False) -> Native:
        filtered_values = [v for v in self.get_values() if function(v)]
        result = self.set_values(filtered_values, inplace=inplace) or self
        return self._assume_native(result)

    def filter_values_defined(self) -> Native:
        return self.filter_values(nm.is_defined)

    def filter_values_nonzero(self) -> Native:
        return self.filter_values(nm.is_nonzero)

    def condition_values(self, function: Callable) -> Native:
        return self.map_values(
            lambda v: v if function(v) else None,
        )

    @staticmethod
    def _get_mapped_items(function: Callable, *values) -> Iterable:
        return map(function, *values)

    def _apply_map_inplace(self, function: Callable) -> Native:
        for n, i in enumerate(self.get_items()):
            item = function(i)
            self.set_item_inplace(n, item)
        return self

    def map(self, function: Callable, inplace: bool = False, validate: bool = False) -> Native:
        if inplace:
            return self._apply_map_inplace(function) or self
        else:
            items = self._get_mapped_items(function, self.get_items())
            return self.set_items(items, inplace=inplace, validate=validate)

    def map_values(self, function: Callable, inplace: bool = False) -> Native:
        mapped_values = self._get_mapped_items(function, self.get_values())
        result = self.set_values(mapped_values, inplace=inplace)
        return self._assume_native(result)

    def map_zip_values(self, function: Callable, *series, inplace: bool = False) -> Native:
        series_values = [s.get_values() for s in series]
        mapped_values = self._get_mapped_items(function, self.get_values(), *series_values)
        return self.set_values(mapped_values, inplace=inplace) or self

    def map_extend_zip_values(self, function: Callable, inplace: bool = False, *series) -> Native:
        count = max([s.get_count() for s in [self] + list(series)])
        extended_series = [s.set_count(count, inplace=inplace) for s in series]
        return self.set_count(
            count, inplace=inplace,
        ).map_zip_values(
            function, *extended_series, inplace=inplace,
        ) or self

    def map_optionally_extend_zip_values(
            self,
            function: Callable,
            extend: bool,
            *series,
            inplace: bool = False,
    ) -> Native:
        if extend:
            return self.map_extend_zip_values(function, *series, inplace=inplace)
        else:
            return self.map_zip_values(function, *series, inplace=inplace)

    def apply(self, function: Callable, inplace: bool = False) -> Native:
        return self.apply_to_values(function, inplace=inplace)

    def apply_to_values(self, function: Callable, inplace: bool = False) -> Native:
        values = function(self.get_values())
        series = self.set_values(values, inplace=inplace)
        return self._assume_native(series)

    def assume_numeric(self, validate: bool = False) -> NumericSeriesInterface:
        series_class = SeriesType.NumericSeries.get_class()
        return series_class(self.get_values(), validate=validate)

    def to_numeric(self, inplace: bool = False) -> Native:
        series = self.map_values(float, inplace=inplace) or self
        return series.assume_numeric()

    def assume_dates(self, validate: bool = False, set_closure: bool = False) -> Native:
        series_class = SeriesType.DateSeries.get_class()
        return series_class(self.get_values(), validate=validate, set_closure=set_closure)

    def to_dates(self, as_iso_date: bool = False) -> Native:
        series_class = SeriesType.DateSeries.get_class()
        series = series_class(self.get_values(), validate=False, sort_items=False).to_dates(as_iso_date=as_iso_date)
        return self._assume_native(series)

    def assume_unsorted(self) -> Native:
        return self

    def assume_sorted(self) -> SortedSeriesInterface:
        series_class = SeriesType.SortedSeries.get_class()
        return series_class(self.get_values(), sort_items=False, validate=False)

    def sort(self, inplace: bool = False) -> SortedSeriesInterface:
        values = self.get_values()
        if inplace:
            values = sorted(values)
            result = self.set_values(values, inplace=False, validate=False)
            return self._assume_sorted(result) or self
        else:
            series_class = SeriesType.SortedSeries.get_class()
            return series_class(values, sort_items=True, validate=False)

    def is_sorted(self, check=True) -> bool:
        if check:
            prev = None
            for v in self.get_values():
                if prev is not None:
                    if v < prev:
                        return False
                prev = v
            return True
        else:
            return DEFAULT_SORTED

    def is_numeric(self, check: bool = False) -> bool:
        if check:
            return self.assume_numeric(validate=False).has_valid_items()
        else:
            return DEFAULT_NUMERIC

    @staticmethod
    def get_names() -> list:
        return ['value']

    def get_dataframe(self) -> nm.DataFrame:
        return nm.get_dataframe(self.get_values(), columns=self.get_names())

    @staticmethod
    def _assume_native(series) -> Native:
        return series

    @staticmethod
    def _assume_sorted(series) -> SortedSeriesInterface:
        return series


SeriesType.add_classes(AnySeries)
