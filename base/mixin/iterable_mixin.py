from abc import ABC
from typing import Optional, Callable, Iterable, Generator, Iterator, Sequence, Sized, Union, Any
from itertools import chain, tee
from datetime import datetime

try:  # Assume we're a submodule in a package.
    from utils import algo, arguments as arg
    from utils.algo import JoinType
    from functions.secondary import item_functions as fs
    from base.interfaces.iterable_interface import IterableInterface, OptionalFields
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import algo, arguments as arg
    from ...utils.algo import JoinType
    from ...functions.secondary import item_functions as fs
    from ..interfaces.iterable_interface import IterableInterface, OptionalFields

Native = IterableInterface
How = Union[JoinType, str]

ARRAY_TYPES = list, tuple


class IterableMixin(IterableInterface, ABC):
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
            raise AttributeError('Object {} has no attribute set_count()'.format(self))
        if not (inplace or result):
            result = self
        return result

    def get_count(self) -> Optional[int]:
        items = self.get_items()
        if isinstance(items, Sized):
            return len(items)

    def get_str_count(self, default: str = '(iter)') -> str:
        count = self.get_count()
        if count is None:
            return default
        else:
            return str(count)

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

    def set_items(self, items: Iterable, inplace: bool) -> Optional[Native]:
        if inplace:
            self.set_data(items, inplace=True)
        else:
            return self.make_new(items)

    def get_items(self) -> Iterable:
        return self.get_data()

    def get_list(self) -> list:
        return list(self.get_items())

    def get_iter(self) -> Generator:
        yield from self.get_items()

    def __iter__(self):
        return self.get_iter()

    def get_one_item(self):
        if self.is_sequence() and self.has_items():
            return self.get_list()[0]
        for i in self.get_iter():
            return i

    def get_description(self) -> str:
        return '{} items'.format(self.get_str_count())

    def _get_enumerated_items(self, item_type=arg.AUTO) -> Generator:
        if item_type == 'Any' or item_type == 'Any' or not arg.is_defined(item_type):
            items = self.get_items()
        elif hasattr(self, 'get_items_of_type'):
            items = self.get_items_of_type(item_type)
        else:
            items = self.get_items()
            if hasattr(self, 'get_item_type'):
                received_item_type = self.get_item_type()
                assert item_type == received_item_type, '{} != {}'.format(item_type, received_item_type)
        for n, i in enumerate(items):
            yield n, i

    def _get_first_items(self, count: int = 1, item_type=arg.AUTO) -> Generator:
        for n, i in self._get_enumerated_items(item_type=item_type):
            yield i
            if n + 1 >= count:
                break

    def _get_second_items(self, skip: int = 1) -> Generator:
        for n, i in self._get_enumerated_items():
            if n >= skip:
                yield i

    def _get_last_items(self, count: int = 10) -> list:
        count = abs(count)
        items = list()
        for i in self.get_items():
            if len(items) >= count:
                items.pop(0)
            items.append(i)
        return items

    def take(self, count: Union[int, bool] = 1) -> Native:
        if (count and isinstance(count, bool)) or not arg.is_defined(count):  # True, None, AUTO
            return self
        elif isinstance(count, int):
            if count > 0:
                items = self._get_first_items(count)
            elif count < 0:
                items = self._get_last_items(-count)
            else:  # count in (0, False)
                items = list()
            kwargs = dict()
            if self.is_in_memory():
                if not arg.is_in_memory(items):
                    items = list(items)
                if self._has_count_attribute():
                    kwargs['count'] = len(items)
            return self.make_new(items, **kwargs)
        else:
            raise TypeError('Expected count as int or boolean, got {}'.format(count))

    def skip(self, count: int = 1) -> Native:
        if self.get_count() and count >= self.get_count():
            next_items = list()
        else:
            next_items = self.get_items()[count:] if self.is_in_memory() else self._get_second_items(count)
        kwargs = dict()
        if self._has_count_attribute():
            old_count = self.get_count()
            if old_count:
                new_count = old_count - count
                kwargs['count'] = new_count
        return self.make_new(next_items, **kwargs)

    def head(self, count: int = 10) -> Native:
        return self.take(count)  # alias

    def tail(self, count: int = 10) -> Native:
        return self.take(-count)

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

    def copy(self) -> Native:
        items = self.get_items()
        if hasattr(items, 'copy'):
            items = items.copy()
        else:
            items = self._get_tee_items()
        return self.make_new(items)

    def make_new(self, *args, count: Optional[int] = None, ex: OptionalFields = None, **kwargs) -> Native:
        if args:
            assert len(args) == 1, 'Expected one position argument (items), got *{}'.format(args)
            items = args[0]
        elif 'items' in kwargs:
            items = kwargs['items']
        else:
            raise AttributeError('items is mandatory argument for IterableMixin.make_new()')
        if self._has_count_attribute():
            if count is None and isinstance(items, ARRAY_TYPES):
                count = len(items)
            kwargs['count'] = count
        return super().make_new(items, ex=ex, **kwargs)

    def _has_count_attribute(self) -> bool:
        if hasattr(self.__init__, '__annotations__'):
            return 'count' in self.__init__.__annotations__

    def add(self, obj_or_items: Union[Native, Iterable], before: bool = False, **kwargs) -> Native:
        if isinstance(obj_or_items, Iterable):
            items = obj_or_items
        elif hasattr(obj_or_items, 'get_items'):
            items = obj_or_items.get_items()
        else:
            raise TypeError('Expected Iterable or IterableMixin, got {}'.format(self))
        return self.add_items(items, before=before)

    def add_items(self, items: Iterable, before: bool = False) -> Native:
        old_items = self.get_items()
        new_items = items
        if before:
            chain_items = chain(new_items, old_items)
        else:
            chain_items = chain(old_items, new_items)
        kwargs = dict()
        if isinstance(items, Sized) and isinstance(items, ARRAY_TYPES):
            old_count = self.get_count()
            try:
                new_count = len(items)
            except TypeError as e:
                raise TypeError('{}: {}'.format(e, items))
            if old_count is not None:
                kwargs['count'] = old_count + new_count
        if self.is_in_memory() and arg.is_in_memory(items):
            chain_items = list(chain_items)
        return self.make_new(chain_items, **kwargs)

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
            for n, f in enumerate(self.tee_streams(count))
        ]

    def split_by_boolean(self, func: Callable) -> list:
        return self.split_by_numeric(lambda f: int(bool(func(f))), count=2)

    def split(self, by: Union[int, list, tuple, Callable], count: Optional[int] = None) -> Iterable:  # +
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
            raise TypeError('split(by): by-argument must be int, list, tuple or function, {} received'.format(type(by)))

    @staticmethod
    def _get_next_items(items: Iterable, step: int):
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
            yield self.stream(items, count=len(items))
            items = self._get_next_items(iterable, step=step)

    def _get_filtered_items(self, function: Callable) -> Iterable:
        return filter(function, self.get_items())

    def filter(self, function: Callable) -> Native:
        items = self._get_filtered_items(function)
        return self.make_new(items)

    def _get_mapped_items(self, function: Callable, flat: bool = False) -> Iterable:
        if flat:
            for i in self.get_iter():
                yield from function(i)
        else:
            yield from map(function, self.get_items())

    def map(self, function: Callable) -> Native:
        items = self._get_mapped_items(function, flat=False)
        return self.make_new(items)

    def flat_map(self, function: Callable) -> Native:
        items = self._get_mapped_items(function, flat=True)
        return self.make_new(items)

    def map_side_join(self, right: Native, key, how: How = JoinType.Left, right_is_uniq: bool = True) -> Native:
        key = arg.get_names(key)
        keys = arg.update([key])
        if not isinstance(how, JoinType):
            how = JoinType(how)
        joined_items = algo.map_side_join(
            iter_left=self.get_items(),
            iter_right=right.get_items(),
            key_function=fs.composite_key(keys),
            merge_function=fs.merge_two_items(),
            dict_function=fs.items_to_dict(),
            how=how,
            uniq_right=right_is_uniq,
        )
        if self.is_in_memory():
            joined_items = list(joined_items)
        return self.make_new(joined_items)

    def get_dict(self, key: Callable, value: Callable) -> dict:
        assert isinstance(key, Callable)
        assert isinstance(value, Callable)
        return {key(i): value(i) for i in self.get_items()}

    def get_demo_example(self, count: int = 3) -> Iterable:
        yield from self.copy().take(count).get_items()

    def show(self, *args, **kwargs):
        if hasattr(self, 'log'):
            self.log(str(self), end='\n', verbose=True, truncate=False, force=True)
        else:
            print(self)
        demo_example = self.get_demo_example(*args, **kwargs)
        if isinstance(demo_example, Iterable):
            demo_example = [str(i) for i in demo_example]
            if hasattr(self, 'log'):
                for example_item in demo_example:
                    msg = 'example: {}'.format(example_item)
                    logging_level_info = 20
                    self.log(msg=msg, level=logging_level_info, verbose=False)
            return '\n'.join(demo_example)
        else:
            return demo_example

    def _get_property(self, name, *args, **kwargs) -> Any:
        if callable(name):
            value = name(self)
        elif isinstance(name, str):
            meta = self.get_meta()
            if name in meta:
                value = meta.get(name)
            else:
                try:
                    getter = self.__getattribute__(name)
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
        value = self._get_property(stream_function)
        if key is not None:
            value = {key: value}
        if hasattr(self, 'log'):
            self.log(value, verbose=show)
        elif show:
            print(value)

        if callable(external_object):
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
            raise TypeError('external_object must be callable, list or dict')
        return self

    def print(self, stream_function: Union[Callable, str] = 'get_count', *args, **kwargs) -> Native:
        value = self._get_property(stream_function, *args, **kwargs)
        if hasattr(self, 'log'):
            self.log(value, end='\n', verbose=True)
        else:
            print(value)
        return self
