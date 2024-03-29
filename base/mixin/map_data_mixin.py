from abc import ABC
from typing import Optional, Iterable, Generator, Sequence, Union
from itertools import chain

try:  # Assume we're a submodule in a package.
    from base.classes.typing import Count, Class
    from base.functions.errors import get_type_err_msg
    from base.constants.chars import EMPTY, REPR_DELIMITER
    from base.classes.enum import DynamicEnum, ClassType
    from base.interfaces.data_interface import SimpleDataInterface
    from base.mixin.data_mixin import Key, Item, KEY, VALUE, DESCRIPTION_COLS
    from base.mixin.iter_data_mixin import IterDataMixin
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.typing import Count, Class
    from ..functions.errors import get_type_err_msg
    from ..constants.chars import EMPTY, REPR_DELIMITER
    from ..classes.enum import DynamicEnum, ClassType
    from ..interfaces.data_interface import SimpleDataInterface
    from .data_mixin import Key, Item, KEY, VALUE, DESCRIPTION_COLS
    from .iter_data_mixin import IterDataMixin

Native = Union[SimpleDataInterface, IterDataMixin]


class MapDataMixin(IterDataMixin, ABC):
    def _get_data(self) -> dict:
        assert isinstance(self, SimpleDataInterface) or hasattr(self, 'get_data')
        return self.get_data()

    @staticmethod
    def _get_root_data_class() -> Class:
        return dict

    @staticmethod
    def get_first_level_key_classes() -> tuple:
        return str, DynamicEnum

    def get_first_level_key_default_class(self) -> Optional[Class]:
        key_classes = self.get_first_level_key_classes()
        if key_classes:
            return key_classes[0]

    @staticmethod
    def get_first_level_key_default_order() -> Optional[Iterable]:
        return None

    def get_casted_first_level_key(self, key: Key, strict: bool = False) -> Key:
        key_default_class = self.get_first_level_key_default_class()
        if key_default_class:
            if strict:
                key_classes = key_default_class
            else:
                key_classes = self.get_first_level_key_classes()
            if isinstance(key, key_classes):
                return key
            else:
                return key_default_class(key)
        if strict:
            msg = 'key classes not defined'
            raise TypeError(self._get_call_prefix(self.get_casted_first_level_key, key) + msg)
        else:
            return key

    def get_first_level_keys(self) -> Iterable:
        return self._get_data().keys()

    def get_sorted_first_level_keys(self) -> list:
        keys = self.get_first_level_keys()
        default_order = self.get_first_level_key_default_order()
        if default_order:
            sorted_keys = list()
            for k in default_order:
                if k in keys:
                    sorted_keys.append(k)
            for k in keys:
                if k not in sorted_keys:
                    sorted_keys.append(k)
            return sorted_keys
        else:
            return sorted(keys)

    def _get_first_level_iter(self, as_pairs: bool = False) -> Generator:
        yield from self._get_first_level_items(as_pairs=as_pairs)

    def _get_first_level_items(self, as_pairs: bool = False) -> Iterable:
        if as_pairs:
            return self._get_data().items()
        else:
            return self._get_data().values()

    def _get_first_level_list(self, as_pairs: bool = False) -> list:
        return list(self._get_first_level_items(as_pairs=as_pairs))

    def _get_first_level_seq(self, as_pairs: bool = False) -> Sequence:
        first_level_items = self._get_first_level_items(as_pairs=as_pairs)
        if isinstance(first_level_items, Sequence):
            return first_level_items
        else:
            return list(first_level_items)

    def get_first_level_value_by_key(self, key: Key, default: Item = None) -> Item:
        return self._get_data().get(key, default)

    def get(self, key: Key, default: Item = None) -> Item:
        return self.get_first_level_value_by_key(key, default)

    def keys(self) -> Iterable:
        return self.get_first_level_keys()

    def values(self) -> Iterable:
        return self._get_first_level_items(as_pairs=False)

    def items(self) -> Iterable:
        return self._get_first_level_items(as_pairs=True)

    def __getitem__(self, item) -> Item:
        return self._get_data()[item]


class MultiMapDataMixin(MapDataMixin, ABC):
    @staticmethod
    def _get_max_depth() -> int:
        return 1

    def _get_first_level_item_classes(self) -> tuple:
        return dict, MapDataMixin

    @staticmethod
    def get_second_level_item_classes() -> tuple:
        return tuple()

    def get_second_level_keys(self, key=None) -> Iterable:
        paris = self.get_second_level_items(key=key, as_pairs=True)
        return map(lambda i: i[0], paris)

    def get_second_level_items(self, key=None, as_pairs: bool = False) -> Iterable:
        iter_values = list()
        if key is None:
            keys = self.get_first_level_keys()
        else:
            keys = [key]
        for key in keys:
            second_level_data = self._get_data()[key]
            if isinstance(second_level_data, MapDataMixin):
                iter_values.append(second_level_data._get_first_level_items(as_pairs=as_pairs))
            elif isinstance(second_level_data, dict):
                if as_pairs:
                    iter_values.append(second_level_data.items())
                else:
                    iter_values.append(second_level_data.values())
            else:
                msg = get_type_err_msg(second_level_data, (MapDataMixin, dict), arg=f'data[{key}]')
                raise TypeError(msg)
        return chain(*iter_values)

    def get_second_level_iter(self, key: Optional[Key] = None, as_pairs: bool = False) -> Generator:
        if key is None:
            keys = self.get_first_level_keys()
        else:
            keys = [key]
        for key in keys:
            second_level_data = self._get_data()[key]
            if isinstance(second_level_data, MapDataMixin):
                yield from second_level_data._get_first_level_items(as_pairs=as_pairs)
            elif isinstance(second_level_data, dict):
                if as_pairs:
                    yield from second_level_data.items()
                else:
                    yield from second_level_data.values()
            else:
                template = 'expected second_level_data as Map (MapDataMixin or dict), got {value}'
                msg = template.format(value=second_level_data)
                raise TypeError(self._get_call_prefix(self.get_second_level_iter, arg=key) + msg)

    def get_second_level_value_by_key(self, key: Key, default: Item = None) -> Item:
        for i in self._get_first_level_items(as_pairs=False):
            result = i.get(key)
            if result is not None:
                return result
        return default

    def get_from_data(self, key: Key, subkey: Item = None) -> Item:
        data = self._get_data()
        if key not in data:
            data[key] = dict()
        if subkey is None:
            return data[key]
        else:
            return data[key].get(subkey)

    def get_item(self, key: Key, subkey: Key, skip_missing: Optional[bool] = None, default=None):
        if skip_missing is None:  # not isinstance(skip_missing, bool)
            skip_missing = default is not None
        data_dict = self.get_from_data(key)
        if subkey in data_dict:
            return data_dict[subkey]
        elif skip_missing:
            return default
        else:
            formatter = '{cls}.get_item({key}, {subkey}): item {subkey} not exists: {existing}'
            msg = formatter.format(cls=self.__class__.__name__, key=key, subkey=subkey)
            raise IndexError(msg)

    def add_item(self, key: Key, subkey: Key, value: Item, allow_override: bool = False) -> Union[Native, MapDataMixin]:
        data_dict = self.get_from_data(key)
        if not allow_override:
            if subkey in data_dict:
                existing = data_dict[subkey]
                arg_str = '{key}, {subkey}, {value}'.format(key=key, subkey=subkey, value=value)
                formatter = 'item {subkey} already exists: {existing}'
                msg = formatter.format(subkey=subkey, existing=existing)
                raise ValueError(self._get_call_prefix(self.add_item, arg=arg_str) + msg)
        data_dict[subkey] = value
        return self

    def add_to_data(self, key: Key, value: Item = None, **kwargs) -> Union[Native, MapDataMixin]:
        if not (value or kwargs):
            return self
        key = self.get_casted_first_level_key(key)
        data = self._get_data()
        if key not in data:
            data[key] = dict()
        data_dict = data[key]
        assert isinstance(data_dict, dict), get_type_err_msg(expected=dict, got=data_dict, arg='data')
        added_items = list()
        if value:
            added_items += list(value.items())
        added_items += list(kwargs.items())
        if isinstance(key, ClassType) or hasattr(key, 'get_class'):
            subkey_class = key.get_class()
        else:
            subkey_class = str
        for k, v in added_items:
            if isinstance(k, str) and subkey_class != str:
                try:
                    k = subkey_class(k)
                except TypeError as e:
                    raise TypeError(f'{key}->{k}: {e}')
            data_dict[k] = v
        return self

    def get(self, key: Key, default: Item = None) -> Item:
        result = self.get_first_level_value_by_key(key)
        if result is not None:
            return result
        result = self.get_second_level_value_by_key(key)
        if result:
            return result
        return default

    def keys(self) -> Iterable:
        return chain(self.get_first_level_keys(), self.get_second_level_keys())

    def values(self) -> Iterable:
        return chain(self._get_first_level_items(as_pairs=False), self.get_second_level_items())

    def items(self) -> Iterable:
        return self._get_first_level_items(as_pairs=True)

    def _get_item_classes(self, level: int = -1) -> tuple:
        if level == 2:
            return self.get_second_level_item_classes()
        else:
            return super()._get_item_classes(level)
