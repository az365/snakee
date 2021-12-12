from typing import Optional, Callable, Iterable

try:  # Assume we're a sub-module in a package.
    from series import series_classes as sc
    from functions.primary import numeric as nm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import series_classes as sc
    from ...functions.primary import numeric as nm

Native = sc.AnySeries

DATA_MEMBER_NAMES = ('keys', '_data')


class KeyValueSeries(sc.AnySeries):
    def __init__(
            self,
            keys=[],
            values=[],
            validate=True,
            name=None,
    ):
        self.keys = list(keys)
        super().__init__(
            values=values,
            validate=validate,
            name=name,
        )

    def get_errors(self):
        yield from super().get_errors()
        if not self.has_valid_counts():
            yield 'Keys and values count for {} must be similar'.format(self.get_class_name())

    def has_valid_counts(self):
        return len(self.get_keys()) == len(self.get_values())

    @classmethod
    def _get_data_member_names(cls):
        return DATA_MEMBER_NAMES

    @classmethod
    def from_items(cls, items: Iterable) -> Native:
        series = cls()
        for k, v in items:
            series.get_keys().append(k)
            series.get_values().append(v)
        return series

    @classmethod
    def from_dict(cls, my_dict) -> Native:
        series = cls()
        for k in sorted(my_dict):
            series.append_pair(k, my_dict[k], inplace=True)
        return series.assume_sorted()

    def key_series(self) -> Native:
        return sc.AnySeries(self.get_keys())

    def value_series(self) -> Native:
        return sc.AnySeries(self.get_values())

    def get_value_by_key(self, key, default=None):
        return self.get_dict().get(key, default)

    def get_keys(self) -> list:
        return self.keys

    def set_keys(self, keys: list, inplace: bool) -> Optional[Native]:
        if inplace:
            self.keys = keys
        else:
            return self.new(
                keys=keys,
                values=self.get_values(),
            )

    def get_items(self) -> Iterable:
        return zip(self.get_keys(), self.get_values())

    def set_items(self, items: Iterable, inplace: bool) -> Optional[Native]:
        if inplace:
            keys, values = self._split_keys_and_values(items)
            self.set_keys(keys, inplace=True)
            self.set_values(values, inplace=True)
        else:
            return self.from_items(items)

    @staticmethod
    def _split_keys_and_values(items: Iterable) -> tuple:
        keys = list()
        values = list()
        for k, v in items:
            keys.append(k)
            values.append(v)
        return keys, values

    def get_dict(self) -> dict:
        return dict(self.get_items())

    def get_arg_min(self):
        min_value = None
        key_for_min_value = None
        for k, v in self.get_items():
            if min_value is None or v < min_value:
                min_value = v
                key_for_min_value = k
        return key_for_min_value

    def get_arg_max(self):
        max_value = None
        key_for_max_value = None
        for k, v in self.get_items():
            if max_value is None or v > max_value:
                max_value = v
                key_for_max_value = k
        return key_for_max_value

    def append(self, item, inplace: bool) -> Optional[Native]:
        assert len(item) == 2, 'Len of pair mus be 2 (got {})'.format(item)
        key, value = item
        return self.append_pair(key, value, inplace)

    def append_pair(self, key, value, inplace: bool) -> Optional[Native]:
        if inplace:
            self.get_keys().append(key)
            self.get_values().append(value)
        else:
            new = self.copy()
            new.get_keys().append(key)
            new.get_values().append(value)
            return new

    def add(self, key_value_series, to_the_begin: bool = False, inplace: bool = False) -> Optional[Native]:
        appropriate_classes = KeyValueSeries, sc.KeyValueSeries, sc.DateNumericSeries
        is_appropriate = isinstance(key_value_series, appropriate_classes)
        assert is_appropriate, 'expected {}, got {}'.format(appropriate_classes, key_value_series)
        if to_the_begin:
            keys = key_value_series.get_keys() + self.get_keys()
            values = key_value_series.get_values() + self.get_values()
        else:
            keys = self.get_keys() + key_value_series.get_keys()
            values = self.get_values() + key_value_series.get_values()
        if inplace:
            self.set_keys(keys, inplace=True)
            self.set_values(keys, inplace=True)
        else:
            return self.new(
                keys=keys,
                values=values,
                save_meta=True,
            )

    def filter_pairs(self, function: Callable, inplace: bool = False) -> Optional[Native]:
        keys, values = list(), list()
        for k, v in self.get_items():
            if function(k, v):
                keys.append(k)
                values.append(v)
        if inplace:
            self.set_keys(keys)
            self.set_values(values)
        else:
            return self.new(
                keys=keys,
                values=values,
            )

    def filter_keys(self, function: Callable, inplace: bool = False) -> Optional[Native]:
        return self.filter_pairs(lambda k, v: function(k), inplace=inplace)

    def filter_values(self, function: Callable, inplace: bool = False) -> Optional[Native]:
        return self.filter_pairs(lambda k, v: function(v), inplace=inplace)

    def filter_values_defined(self, inplace: bool = False) -> Optional[Native]:
        return self.filter_values(nm.is_defined, inplace=inplace)

    def filter_keys_defined(self, inplace: bool = False):
        return self.filter_keys(nm.is_defined, inplace=inplace)

    def filter_keys_between(self, key_min, key_max, inplace: bool = False):
        return self.filter_keys(lambda k: key_min <= k <= key_max, inplace=inplace)

    def map_keys(self, function: Callable, sorting_changed: bool = False, inplace: bool = False):
        keys = self.key_series().map(function)
        return self.set_keys(keys, inplace=inplace)

    def assume_date_numeric(self):
        return sc.DateNumericSeries(**self.get_props())

    def assume_sorted(self):
        return sc.SortedKeyValueSeries(**self.get_props())

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
                return result.assume_sorted()

    def group_by_keys(self) -> Native:
        dict_groups = dict()
        for k, v in self.get_items():
            dict_groups[k] = dict_groups.get(k, []) + [v]
        return __class__().from_dict(dict_groups)

    def sum_by_keys(self) -> Native:
        return self.group_by_keys().map(sum)

    def mean_by_keys(self):
        return self.group_by_keys().map(
            lambda a: sc.AnySeries(a).filter_values_defined().get_mean(),
        )

    @staticmethod
    def get_names() -> tuple:
        return 'key', 'value'

    def get_dataframe(self) -> nm.DataFrame:
        return nm.get_dataframe(
            data=self.get_list(),
            columns=self.get_names(),
        )

    def plot(self, fmt='-'):
        nm.plot(self.get_keys(), self.get_values(), fmt=fmt)

    def __repr__(self):
        count, keys, values = self.get_count(), self.get_keys(), self.get_values()
        if count > 3:
            keys = keys[:2] + ['...'] + keys[-1:]
            values = values[:2] + ['...'] + values[-1:]
        keys = ', '.join(map(str, keys))
        values = ', '.join(map(str, values))
        return "{}(count={}, keys={}, values={})".format(self.__class__.__name__, count, keys, values)
