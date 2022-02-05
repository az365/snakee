from typing import Optional, Callable, Iterable, Generator, Any

try:  # Assume we're a submodule in a package.
    from functions.primary import numeric as nm
    from series.series_type import SeriesType
    from series.interfaces.key_value_series_interface import KeyValueSeriesInterface, Mutable, MUTABLE
    from series.interfaces.sorted_key_value_series_interface import SortedKeyValueSeriesInterface
    from series.interfaces.date_numeric_series_interface import DateNumericSeriesInterface
    from series.simple.any_series import AnySeries
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...functions.primary import numeric as nm
    from ..series_type import SeriesType
    from ..interfaces.key_value_series_interface import KeyValueSeriesInterface, Mutable, MUTABLE
    from ..interfaces.sorted_key_value_series_interface import SortedKeyValueSeriesInterface
    from ..interfaces.date_numeric_series_interface import DateNumericSeriesInterface
    from ..simple.any_series import AnySeries

Native = KeyValueSeriesInterface

DATA_MEMBER_NAMES = '_keys', '_data'
META_MEMBER_MAPPING = dict(_data='values')


class KeyValueSeries(AnySeries, KeyValueSeriesInterface):
    def __init__(
            self,
            keys: Optional[Iterable] = None,
            values: Optional[Iterable] = None,
            set_closure: bool = False,
            validate: bool = True,
            name: Optional[str] = None,
    ):
        self._keys = self._get_optional_copy(keys, role='keys', set_closure=set_closure)
        super().__init__(values=values, set_closure=set_closure, validate=validate, name=name)

    def get_errors(self) -> Generator:
        yield from super().get_errors()
        if not self.has_valid_counts():
            yield 'Keys and values count for {} must be similar'.format(self.get_class_name())

    def has_valid_counts(self) -> bool:
        return len(self.get_keys()) == len(self.get_values())

    @classmethod
    def _get_data_member_names(cls) -> tuple:
        return DATA_MEMBER_NAMES

    @classmethod
    def _get_meta_member_mapping(cls) -> dict:
        meta_member_mapping = super()._get_meta_member_mapping()
        meta_member_mapping.update(META_MEMBER_MAPPING)
        return meta_member_mapping

    @classmethod
    def from_items(cls, items: Iterable) -> Native:
        series = cls()
        for k, v in items:
            series.get_keys().append(k)
            series.get_values().append(v)
        return series

    @classmethod
    def from_dict(cls, my_dict: dict) -> Native:
        series = cls()
        for k in sorted(my_dict):
            series.append_pair(k, my_dict[k], inplace=True)
        return series.assume_sorted()

    def key_series(self, set_closure: bool = False) -> AnySeries:
        series_class = SeriesType.AnySeries.get_class()
        return series_class(self.get_keys(), set_closure=set_closure)

    def value_series(self, set_closure: bool = False, name: Optional[str] = None) -> AnySeries:
        series_class = SeriesType.AnySeries.get_class()
        return series_class(self.get_values(), set_closure=set_closure, validate=False, name=name)

    def get_value_by_key(self, key: Any, default: Any = None):
        return self.get_dict().get(key, default)

    def get_keys(self) -> list:
        return self._keys

    def set_keys(self, keys: Iterable, inplace: bool, set_closure: bool = False) -> Optional[Native]:
        if inplace:
            keys = self._get_optional_copy(keys, role='keys', set_closure=set_closure)
            self._keys = keys
        else:
            result = self.new(keys=keys, values=self.get_values())
            return self._assume_native(result)

    def set_values(self, values: Iterable, inplace: bool, set_closure: bool = False, validate: bool = False) -> Native:
        if inplace:
            return super().set_values(values, set_closure=set_closure, inplace=True) or self
        else:
            return self.set_data(
                self.get_keys(), values=values,
                reset_dynamic_meta=True, validate=validate,
                set_closure=set_closure, inplace=False,
            )

    def get_items(self) -> Iterable:
        return zip(self.get_keys(), self.get_values())

    def get_dict(self, **kwargs) -> dict:
        assert not kwargs, 'got {}'.format(kwargs)
        return dict(self.get_items())

    def set_items(self, items: Iterable, inplace: bool, **kwargs) -> Optional[Native]:
        if inplace:
            keys, values = self._split_keys_and_values(items)
            self.set_keys(keys, inplace=True)
            self.set_values(values, inplace=True)
        else:
            return self.from_items(items)

    def set_item_inplace(self, no: int, *item) -> Native:
        if len(item) == 1:
            key, value = item[0]
        elif len(item) == 2:
            key, value = item
        else:
            raise ValueError('Expected 1 tuple or 2 arguments (item and value), got {}'.format(item))
        keys = self.get_keys()
        values = self.get_values()
        keys[no] = key
        values[no] = value
        return self

    @staticmethod
    def _split_keys_and_values(items: Iterable) -> tuple:
        keys = list()
        values = list()
        for k, v in items:
            keys.append(k)
            values.append(v)
        return keys, values

    def get_arg_min(self) -> Any:
        min_value = None
        key_for_min_value = None
        for k, v in self.get_items():
            if min_value is None or v < min_value:
                min_value = v
                key_for_min_value = k
        return key_for_min_value

    def get_arg_max(self) -> Any:
        max_value = None
        key_for_max_value = None
        for k, v in self.get_items():
            if max_value is None or v > max_value:
                max_value = v
                key_for_max_value = k
        return key_for_max_value

    def append(self, item: Any, inplace: bool) -> Optional[Native]:
        assert len(item) == 2, 'Len of pair mus be 2 (got {})'.format(item)
        key, value = item
        return self.append_pair(key, value, inplace)

    def append_pair(self, key: Any, value: Any, inplace: bool) -> Optional[Native]:
        if inplace:
            self.get_keys().append(key)
            self.get_values().append(value)
        else:
            new = self.copy()
            assert isinstance(new, KeyValueSeries)
            new.get_keys().append(key)
            new.get_values().append(value)
            return self._assume_native(new)

    def add(self, key_value_series: Native, before: bool = False, inplace: bool = False, **kwargs) -> Optional[Native]:
        appropriate_classes = KeyValueSeries, KeyValueSeriesInterface
        is_appropriate = isinstance(key_value_series, appropriate_classes)
        assert is_appropriate, 'expected {}, got {}'.format(appropriate_classes, key_value_series)
        if before:
            keys = key_value_series.get_keys() + self.get_keys()
            values = key_value_series.get_values() + self.get_values()
        else:
            keys = self.get_keys() + key_value_series.get_keys()
            values = self.get_values() + key_value_series.get_values()
        if inplace:
            self.set_keys(keys, inplace=True)
            self.set_values(values, inplace=True)
        else:
            result = self.new(
                keys=keys,
                values=values,
                save_meta=True,
                **kwargs,
            )
            return self._assume_native(result)

    def filter_pairs(self, function: Callable, inplace: bool = False) -> Optional[Native]:
        keys, values = list(), list()
        for k, v in self.get_items():
            if function(k, v):
                keys.append(k)
                values.append(v)
        if inplace:
            self.set_keys(keys, inplace=True)
            self.set_values(values, inplace=True)
        else:
            result = self.new(keys=keys, values=values)
            return self._assume_native(result)

    def filter_keys(self, function: Callable, inplace: bool = False) -> Native:
        return self.filter_pairs(lambda k, v: function(k), inplace=inplace) or self

    def filter_values(self, function: Callable, inplace: bool = False) -> Native:
        return self.filter_pairs(lambda k, v: function(v), inplace=inplace) or self

    def filter_values_defined(self, inplace: bool = False) -> Native:
        return self.filter_values(nm.is_defined, inplace=inplace) or self

    def filter_keys_defined(self, inplace: bool = False) -> Native:
        return self.filter_keys(nm.is_defined, inplace=inplace) or self

    def filter_keys_between(self, key_min, key_max, inplace: bool = False) -> Native:
        return self.filter_keys(lambda k: key_min <= k <= key_max, inplace=inplace) or self

    def map_keys(self, function: Callable, sorting_changed: bool = False, inplace: bool = False) -> Native:
        keys = self.key_series().map(function)
        return self.set_keys(keys, inplace=inplace) or self

    def assume_date_numeric(self) -> DateNumericSeriesInterface:
        series_class = SeriesType.DateNumericSeries.get_class()
        return series_class(**self.get_props())

    def assume_sorted(self) -> SortedKeyValueSeriesInterface:
        series_class = SeriesType.SortedKeyValueSeries.get_class()
        return series_class(**self.get_props())

    def is_sorted(self, check: bool = True) -> bool:
        return self.key_series().is_sorted(check=check)

    def sort_by_keys(self, reverse: bool = False, inplace: bool = False) -> Optional[Native]:
        if inplace:
            items = sorted(zip(self.get_keys(), self.get_values()), reverse=reverse)
            self.set_keys([k for k, v in items], inplace=True)
            self.set_values([v for k, v in items], inplace=True)
        else:
            result = self.__class__.from_items(
                sorted(self.get_items(), reverse=reverse),
            )
            if reverse:
                return result
            else:
                result = result.assume_sorted()
                return self._assume_native(result)

    def group_by_keys(self) -> Native:
        dict_groups = dict()
        for k, v in self.get_items():
            dict_groups[k] = dict_groups.get(k, []) + [v]
        return __class__().from_dict(dict_groups)

    def sum_by_keys(self) -> Native:
        result = self.group_by_keys().map(sum)
        return self._assume_native(result)

    def mean_by_keys(self) -> Native:
        series_class = SeriesType.AnySeries.get_class()
        result = self.group_by_keys().map(
            lambda a: series_class(a).filter_values_defined().get_mean(),
        )
        return self._assume_native(result)

    @staticmethod
    def get_names() -> tuple:
        return 'key', 'value'

    def get_dataframe(self) -> nm.DataFrame:
        return nm.get_dataframe(data=self.get_list(), columns=self.get_names())

    def plot(self, fmt: str = '-') -> None:
        nm.plot(self.get_keys(), self.get_values(), fmt=fmt)

    def __repr__(self):
        count, keys, values = self.get_count(), self.get_keys(), self.get_values()
        if count > 3:
            keys = keys[:2] + ['...'] + keys[-1:]
            values = values[:2] + ['...'] + values[-1:]
        keys = ', '.join(map(str, keys))
        values = ', '.join(map(str, values))
        return "{}(count={}, keys={}, values={})".format(self.__class__.__name__, count, keys, values)

    @staticmethod
    def _assume_native(series) -> Native:
        return series


SeriesType.add_classes(AnySeries, KeyValueSeries)
