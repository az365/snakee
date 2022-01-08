from typing import Optional, Callable, Iterable, Generator, Union, Any

try:  # Assume we're a submodule in a package.
    from series import series_classes as sc
    from functions.primary import numeric as nm, dates as dt
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import series_classes as sc
    from ...functions.primary import numeric as nm, dates as dt

Series = sc.AnySeries
Native = Union[sc.KeyValueSeries, sc.SortedSeries]
Name = Optional[str]


class SortedKeyValueSeries(sc.KeyValueSeries, sc.SortedSeries):
    def __init__(
            self,
            keys: Optional[Iterable] = None,
            values: Optional[Iterable] = None,
            set_closure: bool = False,
            validate: bool = False,
            sort_items: bool = True,
            name: Optional[str] = None,
    ):
        super().__init__(keys=keys, values=values, set_closure=set_closure, validate=False, name=name)
        if sort_items:
            self.sort_by_keys(inplace=True)
        if validate:
            self.validate()

    def get_errors(self) -> Generator:
        yield from super().get_errors()
        if not self.is_sorted(check=True):
            yield 'Keys of {} must be sorted'.format(self.get_class_name())

    @classmethod
    def _get_meta_member_names(cls) -> list:
        return list(super()._get_meta_member_names()) + ['cached_spline']

    def key_series(self, set_closure: bool = False, name: Name = None) -> Series:
        return sc.SortedSeries(self.get_keys(), set_closure=set_closure, validate=False, sort_items=False, name=name)

    def has_key_in_range(self, key: Any):
        return self.get_first_key() <= key <= self.get_last_key()

    def get_first_key(self) -> Any:
        if self.get_count():
            return self.get_keys()[0]

    def get_last_key(self) -> Any:
        if self.get_count():
            return self.get_keys()[-1]

    def get_first_item(self) -> tuple:
        return self.get_first_key(), self.get_first_value()

    def get_last_item(self) -> tuple:
        return self.get_last_key(), self.get_last_value()

    def get_border_keys(self) -> list:
        return [self.get_first_key(), self.get_last_key()]

    def get_mutual_border_keys(self, other: Native) -> list:
        assert isinstance(other, sc.SortedKeyValueSeries)
        first_key = max(self.get_first_key(), other.get_first_key())
        last_key = min(self.get_last_key(), other.get_last_key())
        if first_key < last_key:
            return [first_key, last_key]

    def assume_sorted(self) -> Native:
        return self

    def assume_unsorted(self) -> Series:
        return sc.KeyValueSeries(
            **self._get_data_member_dict()
        )

    def assume_dates(self, validate: bool = False, set_closure: bool = False) -> Native:
        return sc.DateNumericSeries(
            **self._get_data_member_dict(),
            validate=validate,
            set_closure=set_closure,
        )

    def assume_numeric(self, validate: bool = False, set_closure: bool = False) -> Native:
        return sc.SortedNumericKeyValueSeries(
            **self._get_data_member_dict(),
            validate=validate,
            set_closure=set_closure,
        )

    def to_numeric(self, sort_items: bool = True, inplace: bool = False) -> Native:
        series = self.map_keys_and_values(float, float, sorting_changed=False, inplace=inplace) or self
        if sort_items:
            series = series.sort()
        return series.assert_numeric()

    def copy(self) -> Native:
        series = self.new(
            keys=self.get_keys().copy(),
            values=self.get_values().copy(),
            sort_items=False,
            validate=False,
        )
        return self._assume_native(series)

    def map_keys(self, function: Callable, sorting_changed: bool = True, inplace: bool = True) -> Native:
        key_series = self.key_series(set_closure=True).map(function, inplace=inplace, validate=False)
        if inplace:
            result = self
        else:
            result = self.set_keys(key_series, inplace=False)  # set_closure=True ?
        if sorting_changed:
            result = result.assume_unsorted()
        return self._assume_native(result)

    def map_keys_and_values(
            self,
            key_function: Callable,
            value_function: Callable,
            sorting_changed: bool = False,
            inplace: bool = False,
    ) -> Native:
        result = self.map_values(value_function, inplace=inplace) or self
        result = result.map_keys(key_function, sorting_changed, inplace=inplace) or self
        return self._assume_native(result)

    def exclude(self, first_key: Any, last_key: Any, inplace: bool = False) -> Native:
        result = self.filter_keys(lambda k: k < first_key or k > last_key, inplace=inplace) or self
        return self._assume_native(result)

    def span(self, first_key: Any, last_key: Any, inplace: bool = False) -> Native:
        result = self.filter_keys(lambda k: first_key <= k <= last_key, inplace=inplace) or self
        return self._assume_native(result)

    def __repr__(self):
        count, keys, values = self.get_count(), self.get_keys(), self.get_values()
        if count == 0:
            return '{}(0)'.format(self.__class__.__name__)
        elif count == 1:
            keys = keys[0]
            values = values[0]
        else:
            keys = '{}..{}'.format(keys[0], keys[-1])
            values = '{}..{}'.format(values[0], values[-1])
        return '{}({}, {}, {})'.format(self.__class__.__name__, count, keys, values)

    @staticmethod
    def _assume_native(series) -> Native:
        return series
