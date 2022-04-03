from abc import ABC
from typing import Type, Optional, Callable, Iterable, Generator, Sequence, Union, Any
from itertools import chain

try:  # Assume we're a submodule in a package.
    from base.classes.typing import AUTO, Auto, AutoBool, AutoCount
    from base.functions.arguments import get_name
    from base.constants.chars import EMPTY, PY_INDENT, REPR_DELIMITER
    from base.classes.enum import DynamicEnum, ClassType
    from base.interfaces.data_interface import SimpleDataInterface
    from base.mixin.line_output_mixin import LineOutputMixin
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.typing import AUTO, Auto, AutoBool, AutoCount
    from ..functions.arguments import get_name
    from ..constants.chars import EMPTY, PY_INDENT, REPR_DELIMITER
    from ..classes.enum import DynamicEnum, ClassType
    from ..interfaces.data_interface import SimpleDataInterface
    from .line_output_mixin import LineOutputMixin

Native = SimpleDataInterface
Key = Union[DynamicEnum, str]
Item = Any
ItemType = Union[Type, DynamicEnum, str]
Class = Union[Type, Callable]

DESCRIPTION_COL_NAMES = PREFIX, KEY, VALUE, CAPTION = 'prefix', 'key', 'value', 'caption'
DESCRIPTION_COL_LENS = 3, 10, 20, 85  # prefix, key, value, caption
DESCRIPTION_COLS = list(zip(DESCRIPTION_COL_NAMES, DESCRIPTION_COL_LENS))


class DataMixin(LineOutputMixin, ABC):
    def _get_data(self) -> Iterable:
        assert isinstance(self, SimpleDataInterface) or hasattr(self, 'get_data')
        return self.get_data()

    @staticmethod
    def get_max_depth() -> int:
        return 0

    @staticmethod
    def get_root_data_class() -> Class:
        return Any

    def get_item_classes(self, level: int = -1) -> tuple:
        if level < 0:
            level = 1 + self.get_max_depth() - level
            return self.get_item_classes(level)
        if level == 0:
            return self.get_root_data_class(),
        else:
            template = 'Expected level from -{limit} to {limit}, got {value}'
            msg = template.format(limit=self.get_max_depth(), value=level)
            raise ValueError(self._get_call_prefix(self.get_item_classes, arg=level) + msg)

    def get_default_item_class(self, level: int = -1) -> Optional[Class]:
        item_classes = self.get_item_classes(level)
        if item_classes:
            return item_classes[0]

    def set_data(self, data: Iterable, inplace: bool, **kwargs) -> Native:
        data_root_class = self.get_root_data_class()
        if Auto.is_defined(data_root_class):
            assert isinstance(data, data_root_class)
        parent = super()
        if isinstance(parent, SimpleDataInterface) or hasattr(parent, 'set_data'):
            kwargs['inplace'] = inplace
            return parent.set_data(data, **kwargs)
        else:
            raise AttributeError(self._get_call_prefix(self.set_data) + 'parent method not found')

    def has_data(self) -> bool:
        return bool(self._get_data())

    def is_defined(self) -> bool:
        return self.has_data()

    def is_empty(self) -> bool:
        return not self.has_data()

    def _get_call_prefix(self, method: Union[Callable, str, None] = None, arg: Any = '', delimiter: str = ': ') -> str:
        if method:
            template = '{cls}({name}).{method}({arg})'
            if hasattr(self, 'get_name'):
                name = repr(self.get_name())
            else:
                name = ''
            method = get_name(method, or_callable=False)
            prefix = template.format(cls=self.__class__.__name__, name=name, method=method, arg=arg)
        else:
            prefix = repr(self)
        return prefix + delimiter


class IterDataMixin(DataMixin, ABC):
    @staticmethod
    def get_max_depth() -> int:
        return 1

    @staticmethod
    def get_root_data_class() -> Class:
        return Iterable

    @staticmethod
    def get_first_level_item_classes() -> tuple:
        return str, DynamicEnum

    def get_default_first_level_item_class(self) -> Optional[Class]:
        item_classes = self.get_first_level_item_classes()
        if item_classes:
            return item_classes[0]

    def get_first_level_iter(self) -> Generator:
        yield from self._get_data()

    def get_first_level_items(self) -> Iterable:
        return self._get_data()

    def get_first_level_list(self) -> list:
        first_level_items = self.get_first_level_items()
        if isinstance(first_level_items, list):
            return first_level_items
        else:
            return list(first_level_items)

    def get_first_level_seq(self) -> Sequence:
        first_level_items = self.get_first_level_items()
        if isinstance(first_level_items, Sequence):
            return first_level_items
        else:
            return list(first_level_items)

    def get_item_classes(self, level: int = -1) -> tuple:
        if level == 1:
            return self.get_first_level_item_classes()
        else:
            return super().get_item_classes(level)


class MapDataMixin(IterDataMixin, ABC):
    def _get_data(self) -> dict:
        assert isinstance(self, SimpleDataInterface) or hasattr(self, 'get_data')
        return self.get_data()

    @staticmethod
    def get_root_data_class() -> Class:
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
        else:
            return sorted(keys)

    def get_first_level_iter(self, as_pairs: bool = False) -> Generator:
        yield from self.get_first_level_items(as_pairs=as_pairs)

    def get_first_level_items(self, as_pairs: bool = False) -> Iterable:
        if as_pairs:
            return self._get_data().items()
        else:
            return self._get_data().values()

    def get_first_level_list(self, as_pairs: bool = False) -> list:
        return list(self.get_first_level_items(as_pairs=as_pairs))

    def get_first_level_seq(self, as_pairs: bool = False) -> Sequence:
        first_level_items = self.get_first_level_items(as_pairs=as_pairs)
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
        return self.get_first_level_items(as_pairs=False)

    def items(self) -> Iterable:
        return self.get_first_level_items(as_pairs=True)

    def __getitem__(self, item) -> Item:
        return self._get_data()[item]


class MultiMapDataMixin(MapDataMixin, ABC):
    @staticmethod
    def get_max_depth() -> int:
        return 1

    def get_first_level_item_classes(self) -> tuple:
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
                iter_values.append(second_level_data.get_first_level_items(as_pairs=as_pairs))
            elif isinstance(second_level_data, dict):
                if as_pairs:
                    iter_values.append(second_level_data.items())
                else:
                    iter_values.append(second_level_data.values())
            else:
                template = 'expected second_level_data as Map (MapDataMixin or dict), got {value}'
                msg = template.format(value=second_level_data)
                raise TypeError(self._get_call_prefix(self.get_second_level_items, arg=key) + msg)
        return chain(*iter_values)

    def get_second_level_iter(self, key: Optional[Key] = None, as_pairs: bool = False) -> Generator:
        if key is None:
            keys = self.get_first_level_keys()
        else:
            keys = [key]
        for key in keys:
            second_level_data = self._get_data()[key]
            if isinstance(second_level_data, MapDataMixin):
                yield from second_level_data.get_first_level_items(as_pairs=as_pairs)
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
        for i in self.get_first_level_items(as_pairs=False):
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

    def get_item(self, key: Key, subkey: Key, skip_missing: AutoBool = AUTO, default=None):
        skip_missing = Auto.acquire(skip_missing, default is not None)
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
        assert isinstance(data_dict, dict), 'AbstractTerm.add_to_data(): Expected data as dict, got {}'.format(data)
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
                k = subkey_class(k)
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
        return chain(self.get_first_level_items(as_pairs=False), self.get_second_level_items())

    def items(self) -> Iterable:
        return self.get_first_level_items(as_pairs=True)

    def get_item_classes(self, level: int = -1) -> tuple:
        if level == 2:
            return self.get_second_level_item_classes()
        else:
            return super().get_item_classes(level)

    def get_data_description(
            self,
            count: AutoCount = None,
            title: Optional[str] = None,
            max_len: AutoCount = AUTO,
    ) -> Generator:
        count = Auto.acquire(count, None)
        if title:
            yield title
        for key in self.get_sorted_first_level_keys():
            if hasattr(key, 'get_dict_names'):
                k_name, v_name = key.get_dict_names()
                columns = map(lambda f, s: (k_name if f == KEY else v_name if f == VALUE else f, s), DESCRIPTION_COLS)
            else:
                columns = DESCRIPTION_COLS
            items = self.get_second_level_items(key, as_pairs=True)
            if items:
                yield '{key}:'.format(key=get_name(key))
                records = map(
                    lambda k, v: {
                        k_name: get_name(k), v_name: get_name(v),
                        'caption': k.get_caption() if hasattr(k, 'get_caption') else
                        v.get_caption() if hasattr(v, 'get_caption') else '',
                    },
                    items,
                )
                yield from self._get_columnar_lines(records, columns=columns, count=count, max_len=max_len)
