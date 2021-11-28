from typing import Optional, Iterable, Callable

try:  # Assume we're a sub-module in a package.
    from series.abstract_series import AbstractSeries
    from series import series_classes as sc
    from functions.primary import numeric as nm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..abstract_series import AbstractSeries
    from .. import series_classes as sc
    from ...utils import numeric as nm

Native = AbstractSeries

DEFAULT_NUMERIC = False
DEFAULT_SORTED = False


class AnySeries(AbstractSeries):
    def __init__(
            self,
            values=[],
            validate=False,
            name=None,
    ):
        super().__init__(
            values=list(values),
            validate=validate,
            name=name,
        )

    @classmethod
    def _get_meta_member_names(cls):
        return []

    def get_errors(self):
        if not isinstance(self.get_values(), list):
            yield 'Values must be a list'

    def value_series(self):
        return self

    def get_items(self) -> list:
        return self.get_values()

    def set_items(self, items: Iterable, inplace: bool) -> Optional[Native]:
        return self.set_values(items, inplace=inplace)

    def has_items(self):
        return bool(self.get_values())

    def get_count(self):
        return len(self.get_values())

    def get_range_numbers(self):
        return range(self.get_count())

    def set_count(self, count, default=None):
        if count > self.get_count():
            return self.add(
                self.new().set_values([default] * count, inplace=True),
            )
        else:
            return self.slice(0, count)

    def drop_item_no(self, no):
        return self.slice(0, no).add(
            self.slice(no + 1, self.get_count()),
        )

    def get_item_no(self, no, extend=False, default=None):
        if extend:
            if no < self.get_count():
                return self.get_list()[no]
            else:
                return default
        else:
            return self.get_list()[no]

    def get_items_no(self, numbers, extend=False, default=None):
        for no in numbers:
            yield self.get_item_no(no, extend=extend, default=default)

    def get_items_from_to(self, n_start, n_end):
        return self.get_list()[n_start: n_end]

    def slice(self, n_start, n_end, inplace: bool = False):
        items = self.get_items_from_to(n_start, n_end)
        return self.set_items(items, inplace=inplace)

    def crop(self, left_count, right_count):
        return self.slice(
            n_start=left_count,
            n_end=self.get_count() - right_count,
        )

    def items_no(self, numbers, extend=False, default=None, inplace: bool = False):
        items = self.get_items_no(numbers, extend=extend, default=default)
        return self.set_items(items, inplace=inplace)

    def extend(self, series, default=None):
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

    def insert(self, pos, value, inplace=False):
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

    def filter(self, function: Callable, inplace: bool = False):
        filtered_items = filter(function, self.get_items())
        return self.set_items(filtered_items, inplace=inplace)

    def filter_values(self, function: Callable, inplace: bool = False):
        filtered_values = [v for v in self.get_values() if function(v)]
        return self.set_values(filtered_values, inplace=inplace)

    def filter_values_defined(self):
        return self.filter_values(nm.is_defined)

    def filter_values_nonzero(self):
        return self.filter_values(nm.is_nonzero)

    def condition_values(self, function):
        return self.map_values(
            lambda v: v if function else None,
        )

    @staticmethod
    def _get_mapped_items(function, *values):
        return map(function, *values)

    def map(self, function: Callable, inplace: bool = False):
        items = self._get_mapped_items(function, self.get_items())
        return self.set_items(items, inplace=inplace)

    def map_values(self, function, inplace: bool = False):
        mapped_values = self._get_mapped_items(function, self.get_values())
        return self.set_values(mapped_values, inplace=inplace)

    def map_zip_values(self, function, *series, inplace: bool = False):
        mapped_values = self._get_mapped_items(
            function,
            self.get_values(),
            *[s.get_values() for s in series],
        )
        return self.set_values(mapped_values, inplace=inplace)

    def map_extend_zip_values(self, function, *series):
        count = max([s.get_count() for s in [self] + list(series)])
        return self.set_count(count).map_zip_values(
            function,
            *[s.set_count(count) for s in series]
        )

    def map_optionally_extend_zip_values(self, function, extend, *series):
        if extend:
            return self.map_extend_zip_values(function, *series)
        else:
            return self.map_zip_values(function, *series)

    def apply(self, function):
        return self.apply_to_values(function)

    def apply_to_values(self, function: Callable, inplace: bool = False):
        values = function(self.get_values())
        return self.set_values(values, inplace=inplace)

    def assume_numeric(self, validate=False):
        return sc.NumericSeries(
            self.get_values(),
            validate=validate,
        )

    def to_numeric(self):
        return self.map_values(float).assume_numeric()

    def assume_dates(self, validate=False):
        return sc.DateSeries(
            self.get_values(),
            validate=validate,
        )

    def to_dates(self, as_iso_date=False):
        return sc.DateSeries(
            self.get_values(),
            validate=False,
            sort_items=False,
        ).to_dates(
            as_iso_date=as_iso_date,
        )

    def assume_unsorted(self):
        return self

    def assume_sorted(self):
        return sc.SortedSeries(
            self.get_values(),
            sort_items=False,
            validate=False,
        )

    def sort(self):
        return sc.SortedSeries(
            self.get_values(),
            sort_items=True,
            validate=False,
        )

    def is_sorted(self, check=True):
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

    def is_numeric(self, check=False):
        if check:
            return self.assume_numeric(validate=False).has_valid_items()
        else:
            return DEFAULT_NUMERIC

    @staticmethod
    def get_names():
        return ['value']

    def get_dataframe(self):
        return nm.get_dataframe(self.get_values(), columns=self.get_names())
