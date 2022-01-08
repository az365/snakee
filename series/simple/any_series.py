from typing import Optional, Callable, Iterable, Generator, Any

try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from functions.primary import numeric as nm
    from series.abstract_series import AbstractSeries
    from series import series_classes as sc
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...functions.primary import numeric as nm
    from ..abstract_series import AbstractSeries
    from .. import series_classes as sc

Native = AbstractSeries
Series = AbstractSeries

DEFAULT_NUMERIC = False
DEFAULT_SORTED = False


class AnySeries(AbstractSeries):
    def __init__(
            self,
            values: Iterable,
            validate: bool = False,
            name: Optional[str] = None,
    ):
        super().__init__(
            values=values,
            validate=validate,
            name=name,
        )

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

    def set_items(self, items: Iterable, inplace: bool) -> Optional[Native]:
        return self.set_values(items, inplace=inplace)

    def has_items(self) -> bool:
        return bool(self.get_values())

    def get_count(self) -> int:
        return len(self.get_values())

    def get_range_numbers(self) -> Iterable:
        return range(self.get_count())

    def set_count(self, count: int, default: Any = None):
        if count > self.get_count():
            additional_values = [default] * (self.get_count() - count)
            return self.add(additional_values)
        else:
            return self.slice(0, count)

    def drop_item_no(self, no: int) -> Native:
        return self.slice(0, no).add(
            self.slice(no + 1, self.get_count()),
        )

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

    def crop(self, left_count: int, right_count: int) -> Native:
        return self.slice(
            n_start=left_count,
            n_end=self.get_count() - right_count,
        )

    def items_no(self, numbers: Iterable, extend: bool = False, default: Any = None, inplace: bool = False) -> Native:
        items = self.get_items_no(numbers, extend=extend, default=default)
        return self.set_items(items, inplace=inplace)

    def extend(self, series: Native, default: Any = None) -> Native:
        count = series.get_count()
        if self.get_count() < count:
            return self.set_count(count, default)
        else:
            return self

    def intersect(self, series):
        count = series.get_count()
        if self.get_count() > count:
            return self.set_count(count)
        else:
            return self

    def shift(self, distance):
        return self.shift_value_positions(distance)

    def shift_values(self, diff):
        assert isinstance(diff, (int, float))
        return self.map_values(lambda v: v + diff)

    def shift_value_positions(self, distance, default=None):
        if distance > 0:
            return self.__class__(
                values=[default] * distance + self.get_values()
            )
        else:
            return self.slice(n_start=-distance, n_end=self.get_count())

    def append(self, value, inplace):
        if inplace:
            self.get_values().append(value)
        else:
            new = self.copy()
            new.append(value, inplace=True)
            return new

    def preface(self, value, inplace=False):
        return self.insert(
            pos=0,
            value=value,
            inplace=inplace,
        )

    def insert(self, pos: int, value: Any, inplace: bool = False) -> Native:
        if inplace:
            self.get_values().insert(pos, value)
        else:
            new = self.copy()
            new.insert(pos, value, inplace=True)
            return new

    def add(self, series, to_the_begin: bool = False, inplace: bool = False):
        if to_the_begin:
            values = series.get_values() + self.get_values()
        else:
            values = self.get_values() + series.get_values()
        return self.set_values(values=values, inplace=inplace)

    def filter(self, function: Callable, inplace: bool = False) -> Native:
        filtered_items = filter(function, self.get_items())
        return self.set_items(filtered_items, inplace=inplace)

    def filter_values(self, function: Callable, inplace: bool = False) -> Native:
        filtered_values = [v for v in self.get_values() if function(v)]
        return self.set_values(filtered_values, inplace=inplace)

    def filter_values_defined(self) -> Native:
        return self.filter_values(nm.is_defined)

    def filter_values_nonzero(self) -> Native:
        return self.filter_values(nm.is_nonzero)

    def condition_values(self, function: Callable) -> Native:
        return self.map_values(
            lambda v: v if function else None,
        )

    @staticmethod
    def _get_mapped_items(function: Callable, *values) -> Iterable:
        return map(function, *values)

    def map(self, function: Callable, inplace: bool = False) -> Native:
        items = self._get_mapped_items(function, self.get_items())
        return self.set_items(items, inplace=inplace)

    def map_values(self, function: Callable, inplace: bool = False) -> Native:
        mapped_values = self._get_mapped_items(function, self.get_values())
        return self.set_values(mapped_values, inplace=inplace)

    def map_zip_values(self, function: Callable, *series, inplace: bool = False) -> Native:
        series_values = [s.get_values() for s in series]
        mapped_values = self._get_mapped_items(function, self.get_values(), *series_values)
        return self.set_values(mapped_values, inplace=inplace) or self

    def map_extend_zip_values(self, function: Callable, *series) -> Native:
        count = max([s.get_count() for s in [self] + list(series)])
        extended_series = [s.set_count(count, inplace=inplace) for s in series]
        return self.set_count(
            count,
        ).map_zip_values(
            function, *extended_series,
        )

    def map_optionally_extend_zip_values(
            self,
            function: Callable,
            extend: bool,
            *series,
    ) -> Native:
        if extend:
            return self.map_extend_zip_values(function, *series)
        else:
            return self.map_zip_values(function, *series)

    def apply(self, function: Callable) -> Native:
        return self.apply_to_values(function)

    def apply_to_values(self, function: Callable, inplace: bool = False) -> Native:
        values = function(self.get_values())
        return self.set_values(values, inplace=inplace)

    def assume_numeric(self, validate: bool = False) -> Series:
        return sc.NumericSeries(
            self.get_values(),
            validate=validate,
        )

    def to_numeric(self) -> Native:
        return self.map_values(float).assume_numeric()

    def assume_dates(self, validate: bool = False) -> Native:
        return sc.DateSeries(
            self.get_values(),
            validate=validate,
        )

    def to_dates(self, as_iso_date: bool = False) -> Native:
        return sc.DateSeries(
            self.get_values(),
            validate=False,
            sort_items=False,
        ).to_dates(
            as_iso_date=as_iso_date,
        )

    def assume_unsorted(self) -> Native:
        return self

    def assume_sorted(self) -> Native:
        return sc.SortedSeries(
            self.get_values(),
            sort_items=False,
            validate=False,
        )

    def sort(self) -> Native:
        return sc.SortedSeries(
            self.get_values(),
            sort_items=True,
            validate=False,
        )

    def is_sorted(self, check=True) -> Native:
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
