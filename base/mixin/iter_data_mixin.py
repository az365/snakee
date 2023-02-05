from abc import ABC
from typing import Optional, Callable, Iterable, Generator, Iterator, Sequence, Sized, Union, Any
from itertools import chain, tee
from datetime import datetime

try:  # Assume we're a submodule in a package.
    from functions.secondary import item_functions as fs
    from utils.algo import map_side_join
    from base.classes.typing import ARRAY_TYPES, Auto, Class
    from base.classes.enum import DynamicEnum
    from base.constants.chars import PARAGRAPH_CHAR
    from base.functions.arguments import get_names, update, is_in_memory, get_str_from_args_kwargs
    from base.interfaces.iterable_interface import IterableInterface, OptionalFields, Item, JoinType
    from base.mixin.data_mixin import DataMixin, UNK_COUNT_STUB
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...functions.secondary import item_functions as fs
    from ...utils.algo import map_side_join
    from ..classes.typing import ARRAY_TYPES, Auto, Class
    from ..classes.enum import DynamicEnum
    from ..constants.chars import PARAGRAPH_CHAR
    from ..functions.arguments import get_names, update, is_in_memory, get_str_from_args_kwargs
    from ..interfaces.iterable_interface import IterableInterface, OptionalFields, Item, JoinType
    from .data_mixin import DataMixin, UNK_COUNT_STUB

Native = Union[IterableInterface, DataMixin]
How = Union[JoinType, str]

DEFAULT_COUNT = 10
LOGGING_LEVEL_INFO = 20


class IterDataMixin(DataMixin, ABC):
    @staticmethod
    def _get_max_depth() -> int:
        return 1

    @staticmethod
    def _get_root_data_class() -> Class:
        return Iterable

    @staticmethod
    def _get_first_level_item_classes() -> tuple:
        return str, DynamicEnum

    def _get_default_first_level_item_class(self) -> Optional[Class]:
        item_classes = self._get_first_level_item_classes()
        if item_classes:
            return item_classes[0]

    def _get_first_level_iter(self) -> Generator:
        yield from self._get_data()

    def _get_first_level_items(self) -> Iterable:
        return self._get_data()

    def _get_first_level_list(self) -> list:
        first_level_items = self._get_first_level_items()
        if isinstance(first_level_items, list):
            return first_level_items
        else:
            return list(first_level_items)

    def _get_first_level_seq(self) -> Sequence:
        first_level_items = self._get_first_level_items()
        if isinstance(first_level_items, Sequence):
            return first_level_items
        else:
            return list(first_level_items)

    def _get_item_classes(self, level: int = -1) -> tuple:
        if level == 1:
            return self._get_first_level_item_classes()
        else:
            return super()._get_item_classes(level)

    def is_sequence(self) -> bool:
        return isinstance(self.get_items(), Sequence)

    def is_iter(self) -> bool:
        return isinstance(self.get_items(), (Generator, Iterator))

    def is_in_memory(self) -> Optional[bool]:
        if self.is_sequence():
            return True
        elif self.is_iter():
            return False
        elif isinstance(self, (dict, set)):
            return True
        else:
            return None  # don't know

    def to_memory(self) -> Native:
        if self.is_in_memory():
            return self
        else:
            items = list(self.get_items())
            return self.make_new(items)

    def collect(self, inplace: bool = False, **kwargs) -> Native:
        if inplace:
            self._collect_inplace(**kwargs)
            return self
        else:
            return self.to_memory()

    def _collect_inplace(self, **kwargs) -> None:
        items = self.get_list()
        self.set_data(items, inplace=True)
        self._set_count(len(items), inplace=True, skip_missing=True)

    def _set_count(self, count: int, inplace: bool, skip_missing: bool = True) -> Optional[Native]:
        if hasattr(self, 'set_count'):
            if 'inplace' in self.set_count.__annotations__:
                result = self.set_count(count, inplace=inplace)
            else:
                result = self.set_count(count)
        elif skip_missing:
            result = None
        else:
            raise AttributeError(f'Object {self} has no attribute set_count()')
        if not (inplace or result):
            result = self
        return result

    def get_count(self) -> Optional[int]:
        items = self.get_items()
        if isinstance(items, Sized):
            return len(items)
        elif hasattr(super(), 'get_count'):
            return super().get_count()

    def get_str_count(self, default: str = UNK_COUNT_STUB) -> str:
        count = self.get_count()
        if Auto.is_defined(count):
            return str(count)
        else:
            return default

    def is_empty(self) -> Optional[bool]:
        count = self.get_count()
        if count is None:
            return None
        else:
            return count == 0

    def has_items(self) -> Optional[bool]:
        count = self.get_count()
        if count is None:
            return None
        else:
            return count > 0

    def set_items(self, items: Iterable, inplace: bool, count: Optional[int] = None) -> Native:
        if inplace:
            self.set_data(items, inplace=True)
            if Auto.is_defined(count):
                self._set_count(count, inplace=True)
            return self
        else:
            obj = self.set_data(items, inplace=False)
            if Auto.is_defined(count):
                assert isinstance(obj, IterDataMixin) or hasattr(obj, '_set_count')
                obj = obj._set_count(count, inplace=False)
            return obj

    def get_items(self) -> Iterable:
        return self.get_data()

    def get_list(self) -> list:
        return list(self.get_items())

    def get_iter(self) -> Generator:
        items = self.get_items()
        if items:
            yield from items

    def __iter__(self):
        return self.get_iter()

    def get_one_item(self) -> Item:
        if self.is_sequence() and self.has_items():
            return self.get_list()[0]
        for i in self.get_iter():
            return i

    def _get_enumerated_items(self, first: int = 0, item_type=None) -> Generator:
        if item_type == 'Any' or not Auto.is_defined(item_type):
            items = self.get_items()
        elif hasattr(self, 'get_items_of_type'):
            items = self.get_items_of_type(item_type)
        else:
            items = self.get_items()
            if hasattr(self, 'get_item_type'):
                received_item_type = self.get_item_type()
                assert item_type == received_item_type, f'{item_type} != {received_item_type}'
        for n, i in enumerate(items):
            yield n + first, i

    def _get_first_items(self, count: int = 1, item_type=None) -> Generator:
        for n, i in self._get_enumerated_items(first=1, item_type=item_type):
            yield i
            if n >= count:
                break

    def _get_second_items(self, skip: int = 1, item_type=None) -> Generator:
        for n, i in self._get_enumerated_items(first=0, item_type=item_type):
            if n >= skip:
                yield i

    def _get_last_items(self, count: int = DEFAULT_COUNT) -> list:
        count = abs(count)
        items = list()
        for i in self.get_items():
            if len(items) >= count:
                items.pop(0)
            items.append(i)
        return items

    def take(self, count: Union[int, bool] = 1, inplace: bool = False) -> Optional[Native]:
        if (count and isinstance(count, bool)) or not Auto.is_defined(count):  # True, None
            return self
        elif isinstance(count, int):
            if count > 0:
                items = self._get_first_items(count, item_type=None)
            elif count < 0:
                items = self._get_last_items(-count)
            else:  # count in (0, False)
                items = list()
            result_count = None
            if self.is_in_memory():
                if not is_in_memory(items):
                    items = list(items)
                if self._has_count_attribute():  # in init-annotation
                    result_count = len(items)
            return self.set_items(items, count=result_count, inplace=inplace)
        else:
            raise TypeError(f'Expected count as int or boolean, got {count}')

    def skip(self, count: int = 1, inplace: bool = False) -> Native:
        old_count = self.get_count()
        if Auto.is_defined(old_count) and count >= old_count:
            items = list()
        else:
            items = self.get_items()[count:] if self.is_in_memory() else self._get_second_items(count)
        result_count = None
        if self._has_count_attribute():  # in init-annotation
            if old_count:
                result_count = old_count - count
                if result_count < 0:
                    result_count = 0
        return self.set_items(items, count=result_count, inplace=inplace)

    def head(self, count: int = DEFAULT_COUNT, inplace: bool = False) -> Optional[Native]:
        return self.take(count, inplace=inplace)

    def tail(self, count: int = DEFAULT_COUNT, inplace: bool = False) -> Optional[Native]:
        return self.take(-count, inplace=inplace)

    def pass_items(self) -> Native:
        for _ in self.get_iter():
            pass
        return self

    def get_tee_clones(self, count: int = 2) -> list:
        tee_iterators = tee(self.get_items(), count)
        return [self.make_new(t) for t in tee_iterators]

    def _get_tee_items(self) -> Iterable:
        two_iterators = tee(self.get_items(), 2)
        primary_items, tee_items = two_iterators
        self.set_items(primary_items, inplace=True)
        return tee_items

    def copy(self, **kwargs) -> Native:
        items = self.get_items()
        if hasattr(items, 'copy'):
            items = items.copy()
        else:
            items = self._get_tee_items()
        return self.make_new(items, **kwargs)

    def make_new(self, *args, count: Optional[int] = None, ex: OptionalFields = None, **kwargs) -> Native:
        if args:
            assert len(args) == 1, f'Expected one position argument (items), got *{args}'
            items = args[0]
        elif 'items' in kwargs:
            items = kwargs['items']
        else:
            raise AttributeError('items is mandatory argument for IterDataMixin.make_new()')
        if self._has_count_attribute():
            if count is None and isinstance(items, ARRAY_TYPES):
                count = len(items)
            kwargs['count'] = count
        return super().make_new(items, ex=ex, **kwargs)

    def _has_count_attribute(self) -> bool:
        if hasattr(self.__init__, '__annotations__'):
            return 'count' in self.__init__.__annotations__

    def add(
            self,
            obj_or_items: Union[Native, Iterable],
            before: bool = False,
            inplace: bool = False,
            **kwargs
    ) -> Native:
        if isinstance(obj_or_items, Iterable) and not isinstance(obj_or_items, str):
            items = obj_or_items
        elif hasattr(obj_or_items, 'get_items'):
            items = obj_or_items.get_items()
        else:
            items = [obj_or_items]
        return self.add_items(items, before=before, inplace=inplace)

    def add_items(self, items: Iterable, before: bool = False, inplace: bool = False) -> Optional[Native]:
        old_items = self.get_items()
        new_items = items
        if before:
            chain_items = chain(new_items, old_items)
        else:
            chain_items = chain(old_items, new_items)
        result_count = None
        if isinstance(items, Sized) and isinstance(items, ARRAY_TYPES):
            old_count = self.get_count()
            if old_count is not None:
                add_count = len(items)
                result_count = old_count + add_count
        if self.is_in_memory() and is_in_memory(items):
            chain_items = list(chain_items)
        return self.set_items(chain_items, count=result_count, inplace=inplace)

    def append(self, item, inplace: bool = True) -> Native:
        data = self.get_data()
        if inplace and (isinstance(data, list) or hasattr(data, 'append')):
            data.append(item)
            return self
        else:
            items = chain(self.get_items(), [item])
            return self.set_items(items, inplace=inplace)

    def split_by_pos(self, pos: int) -> tuple:
        first_stream, second_stream = self.get_tee_clones(2)
        return first_stream.take(pos), second_stream.skip(pos)

    def split_by_list_pos(self, list_pos: Union[list, tuple]) -> list:
        list_pos = sorted(list_pos)
        count_limits = len(list_pos)
        tee_clones = self.get_tee_clones(count_limits + 1)
        filtered_clones = list()
        prev_pos = 0
        for n, cur_pos in enumerate(list_pos):
            count_items = cur_pos - prev_pos
            cur_clone = tee_clones[n].skip(prev_pos).take(count_items)
            if self._has_count_attribute() and hasattr(cur_clone, 'set_count'):
                if 'inplace' in cur_clone.set_count.__annotations__:
                    cur_clone.set_count(count_items, inplace=True)
                else:
                    cur_clone.set_count(count_items)
            filtered_clones.append(cur_clone)
            prev_pos = cur_pos
        last_clone = tee_clones[-1].skip(list_pos[-1])
        filtered_clones.append(last_clone)
        return filtered_clones

    def split_by_numeric(self, func: Callable, count: int) -> list:
        return [
            f.filter(lambda i, n=n: func(i) == n)
            for n, f in enumerate(self.get_tee_clones(count))
        ]

    def split_by_boolean(self, func: Callable) -> list:
        return self.split_by_numeric(lambda f: int(bool(func(f))), count=2)

    def split(self, by: Union[int, list, tuple, Callable], count: Optional[int] = None) -> Union[list, tuple]:
        if isinstance(by, int):
            return self.split_by_pos(by)
        elif isinstance(by, ARRAY_TYPES):
            return self.split_by_list_pos(by)
        elif isinstance(by, Callable):
            if count:
                return self.split_by_numeric(by, count)
            else:
                return self.split_by_boolean(by)
        else:
            raise TypeError(f'split(by): by-argument must be int, list, tuple or function, got {by}')

    @staticmethod
    def _get_next_items(items: Iterable, step: int) -> list:
        output_items = list()
        for n, i in enumerate(items):
            output_items.append(i)
            if n + 1 >= step:
                break
        return output_items

    def split_to_iter_by_step(self, step: int) -> Generator:
        iterable = self.get_iter()
        items = self._get_next_items(iterable, step=step)
        while items:
            yield self.make_new(items)
            items = self._get_next_items(iterable, step=step)

    def _get_filtered_items(self, function: Callable) -> Iterable:
        return filter(function, self.get_items())

    def filter(self, function: Callable, inplace: bool = False) -> Optional[Native]:
        items = self._get_filtered_items(function)
        return self.set_items(items, inplace=inplace)

    def _get_mapped_items(self, function: Callable, flat: bool = False) -> Iterable:
        if flat:
            for i in self.get_iter():
                yield from function(i)
        else:
            yield from map(function, self.get_items())

    def _apply_map_inplace(self, function: Callable) -> Native:
        items = self.get_items()
        assert isinstance(items, list), f'expected list, got {items}'
        for k, v in enumerate(items):
            items[k] = function(v)
        return self

    def map(self, function: Callable, inplace: bool = False) -> Native:
        if inplace and isinstance(self.get_items(), list):
            return self._apply_map_inplace(function) or self
        else:
            items = self._get_mapped_items(function, flat=False)
            return self.set_items(items, count=self.get_count(), inplace=inplace)

    def flat_map(self, function: Callable, inplace: bool = False) -> Optional[Native]:
        items = self._get_mapped_items(function, flat=True)
        return self.set_items(items, inplace=inplace)

    def map_side_join(
            self,
            right: Native,
            key,
            how: How = JoinType.Left,
            right_is_uniq: bool = True,
            merge_function: Callable = fs.merge_two_items(),
            inplace: bool = False,
    ) -> Native:
        key = get_names(key)
        keys = update([key])
        if not isinstance(how, JoinType):
            how = JoinType(how)
        joined_items = map_side_join(
            iter_left=self.get_items(),
            iter_right=right.get_items(),
            key_function=fs.composite_key(keys),
            merge_function=merge_function,
            dict_function=fs.items_to_dict(),
            how=how,
            uniq_right=right_is_uniq,
        )
        if self.is_in_memory():
            joined_items = list(joined_items)
        return self.set_items(joined_items, inplace=inplace)

    def get_dict(self, key: Callable, value: Callable) -> dict:
        assert isinstance(key, Callable)
        assert isinstance(value, Callable)
        return {key(i): value(i) for i in self.get_items()}

    def get_demo_example(self, count: int = 3) -> Iterable:
        yield from self.copy().take(count).get_items()

    def show(self, *args, **kwargs):
        if hasattr(self, 'log'):
            self.log(str(self), end=PARAGRAPH_CHAR, verbose=True, truncate=False, force=True)
        else:
            print(self)
        demo_example = self.get_demo_example(*args, **kwargs)
        if isinstance(demo_example, Iterable):
            demo_example = [str(i) for i in demo_example]
            if hasattr(self, 'log'):
                for example_item in demo_example:
                    msg = f'example: {example_item}'
                    self.log(msg=msg, level=LOGGING_LEVEL_INFO, verbose=False)
            return PARAGRAPH_CHAR.join(demo_example)
        else:
            return demo_example

    def _get_property(self, name_or_func, *args, **kwargs) -> Any:
        if isinstance(name_or_func, Callable):
            value = name_or_func(self)
        elif isinstance(name_or_func, str):
            meta = self.get_meta()
            if name_or_func in meta:
                value = meta.get(name_or_func)
            else:
                try:
                    getter = getattr(self, name_or_func)
                    value = getter(*args, **kwargs)
                except AttributeError:
                    value = None
        else:
            raise TypeError('property name must be function, meta-field or attribute name')
        return value

    def submit(
            self,
            external_object: Union[list, dict, Callable] = print,
            stream_function: Union[Callable, str] = 'get_count',
            key: Optional[str] = None, show=False,
    ) -> Native:
        display = self.get_display()
        value = self._get_property(stream_function)
        if key is not None:
            value = {key: value}
        if hasattr(self, 'log'):
            self.log(value, verbose=show)
        elif show:
            display.display_item(value)

        if isinstance(external_object, Callable):
            external_object(value)
        elif isinstance(external_object, list):
            external_object.append(value)
        elif isinstance(external_object, dict):
            if isinstance(value, dict):
                external_object.update(value)
            else:
                cur_time = datetime.now().isoformat()
                external_object[cur_time] = value
        else:
            raise TypeError(f'external_object must be callable, list or dict, got {external_object}')
        return self

    def print(
            self,
            stream_function: Union[Callable, str] = '__str__',
            assert_not_none: bool = True,
            *args, **kwargs
    ) -> Native:
        display = self.get_display()
        value = self._get_property(stream_function, *args, **kwargs)
        if value is None:
            if assert_not_none:
                obj_str = repr(self)
                arg_str = get_str_from_args_kwargs(stream_function, *args, **kwargs)
                raise ValueError(f'{obj_str}.print({arg_str}): None received')
            else:
                value = str(value)
        if hasattr(self, 'log'):
            self.log(value, end=PARAGRAPH_CHAR, verbose=True)
        else:
            display.display_item(value)
        return self
