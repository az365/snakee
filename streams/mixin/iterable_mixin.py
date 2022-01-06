from abc import ABC
from typing import Optional, Callable, Iterable, Generator, Union, Any
from inspect import isclass
from itertools import chain, tee
from datetime import datetime

try:  # Assume we're a sub-module in a package.
    from utils import algo, arguments as arg
    from utils.external import pd, DataFrame, get_use_objects_for_output
    from interfaces import (
        IterableStreamInterface, RegularStreamInterface, Stream, StreamType, ItemType, JoinType, How,
        Source, ExtLogger, SelectionLogger, LoggingLevel,
        AUTO, Auto, AutoName, AutoCount, Count, OptionalFields, UniKey,
    )
    from functions.secondary import item_functions as fs
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import algo, arguments as arg
    from ...utils.external import pd, DataFrame, get_use_objects_for_output
    from ...interfaces import (
        IterableStreamInterface, RegularStreamInterface, Stream, StreamType, ItemType, JoinType, How,
        Source, ExtLogger, SelectionLogger, LoggingLevel,
        AUTO, Auto, AutoName, AutoCount, Count, OptionalFields, UniKey,
    )
    from ...functions.secondary import item_functions as fs

Native = IterableStreamInterface
Data = Union[Auto, Iterable]
AutoStreamType = Union[Auto, StreamType]


class IterableStreamMixin(IterableStreamInterface, ABC):
    def get_expected_count(self) -> Count:
        return self.get_count()

    def get_estimated_count(self) -> Count:
        count = self.get_count()
        if not count:
            if hasattr(self, 'get_less_than'):
                count = self.get_less_than()
        return count

    def get_str_count(self) -> str:
        if self.get_count():
            return '{}'.format(self.get_count())
        elif self.get_estimated_count():
            return '<={}'.format(self.get_estimated_count())
        else:
            return '(unknown count)'

    def enumerate(self, native: bool = False) -> Union[Native, Stream]:
        props = self.get_meta()
        if native:
            target_class = self.__class__
        else:
            target_class = StreamType.KeyValueStream.get_class()
            props['value_stream_type'] = self.get_stream_type()
        return target_class(
            self._get_enumerated_items(),
            **props
        )

    def _get_enumerated_items(self, item_type: Union[ItemType, Auto] = AUTO) -> Iterable:
        if arg.is_defined(item_type) and item_type not in (ItemType.Any, ItemType.Auto):
            if hasattr(self, 'get_items_of_type'):
                items = self.get_items_of_type(item_type)
            else:
                assert isinstance(self, RegularStreamInterface) or hasattr(self, 'get_item_type')
                assert item_type == self.get_item_type() or not arg.is_defined(item_type)
                items = self.get_items()
        else:
            items = self.get_items()
        for n, i in enumerate(items):
            yield n, i

    def get_iter(self) -> Generator:
        yield from self.get_items()

    def __iter__(self):
        return self.get_iter()

    def get_one_item(self):
        for i in self.get_iter():
            return i

    def get_description(self) -> str:
        return '{} items'.format(self.get_str_count())

    def _get_first_items(self, count: int = 1, item_type: Union[ItemType, Auto] = AUTO) -> Iterable:
        for n, i in self._get_enumerated_items(item_type=item_type):
            yield i
            if n + 1 >= count:
                break

    def take(self, count: Union[int, bool] = 1) -> Native:
        if (count and isinstance(count, bool)) or not arg.is_defined(count):  # True, None, AUTO
            return self
        elif isinstance(count, int):
            if count > 0:
                items = self._get_first_items(count)
                item_count = min(self.get_count(), count) if self.get_count() else None
                less_than = min(self.get_estimated_count(), count) if self.get_estimated_count() else count
                stream = self.stream(items, count=item_count, less_than=less_than)
            elif count < 0:
                stream = self.tail(count=count)
            else:  # count in (0, False)
                stream = self.stream([], count=0)
            return self._assume_native(stream)

    def skip(self, count: int = 1) -> Native:
        def skip_items(c):
            for n, i in self._get_enumerated_items():
                if n >= c:
                    yield i
        if self.get_count() and count >= self.get_count():
            next_items = list()
        else:
            next_items = self.get_items()[count:] if self.is_in_memory() else skip_items(count)
        less_than = None
        new_count = None
        old_count = self.get_count()
        if old_count:
            new_count = old_count - count
        elif self.get_estimated_count():
            less_than = self.get_estimated_count() - count
        stream = self.stream(next_items, count=new_count, less_than=less_than)
        return self._assume_native(stream)

    def head(self, count: int = 10) -> Native:
        return self.take(count)  # alias

    def tail(self, count: int = 10) -> Native:
        stream = self.stream(self._get_last_items(count))
        return self._assume_native(stream)

    def _get_last_items(self, count: int = 10) -> list:
        items = list()
        for i in self.get_items():
            if len(items) >= count:
                items.pop(0)
            items.append(i)
        return items

    def pass_items(self) -> Native:
        try:
            for _ in self.get_iter():
                pass
        except BaseException as e:
            msg = 'Error while trying to close stream: {}'.format(e)
            self.log(msg=msg, level=LoggingLevel.Warning)
        return self

    def tee_streams(self, n: int = 2) -> list:
        tee_iterators = tee(self.get_items(), n)
        return [self.stream(t) for t in tee_iterators]

    def to_stream(
            self,
            data: Data = AUTO,
            stream_type: AutoStreamType = AUTO,
            ex: OptionalFields = None,
            **kwargs
    ) -> Stream:
        stream_type = arg.delayed_acquire(stream_type, self.get_stream_type)
        if isinstance(stream_type, str):
            stream_class = StreamType(stream_type).get_class()
        elif isclass(stream_type):
            stream_class = stream_type
        else:
            stream_class = stream_type.get_class()
        data = arg.delayed_acquire(data, self.get_data)
        meta = self.get_compatible_meta(stream_class, ex=ex)
        meta.update(kwargs)
        if 'count' not in meta:
            meta['count'] = self.get_count()
        if 'source' not in meta:
            meta['source'] = self.get_source()
        return stream_class(data, **meta)

    @staticmethod
    def _is_stream(obj) -> bool:
        return isinstance(obj, IterableStreamInterface) or (hasattr(obj, 'get_count') and hasattr(obj, 'get_items'))

    def add(self, stream_or_items: Union[Stream, Iterable], before: bool = False, **kwargs) -> Native:
        if self._is_stream(stream_or_items):
            return self.add_stream(
                self._assume_native(stream_or_items),
                before=before,
            )
        else:
            return self.add_items(
                stream_or_items,
                before=before,
            )

    def add_items(self, items: Iterable, before: bool = False) -> Native:
        old_items = self.get_items()
        new_items = items
        if before:
            chain_records = chain(new_items, old_items)
        else:
            chain_records = chain(old_items, new_items)
        if isinstance(items, (list, tuple)):
            count = self.get_count()
            if count:
                count += len(items)
            less_than = self.get_estimated_count()
            if less_than:
                less_than += len(items)
        else:
            count = None
            less_than = None
        stream = self.stream(chain_records, count=count, less_than=less_than)
        return self._assume_native(stream)

    def add_stream(self, stream: Native, before: bool = False) -> Native:
        old_count = self.get_count()
        new_count = stream.get_count()
        if old_count is not None and new_count is not None:
            total_count = new_count + old_count
        else:
            total_count = None
        stream = self.add_items(
            stream.get_items(),
            before=before,
        ).update_meta(
            count=total_count,
        )
        return self._assume_native(stream)

    def count_to_items(self) -> Native:
        return self.add_items(
            [self.get_count()],
            before=True,
        )

    def separate_count(self) -> tuple:
        return (
            self.get_count(),
            self,
        )

    def separate_first(self) -> tuple:
        items = self.get_iter()
        count = self.get_count()
        if count:
            count -= 1
        less_than = self.get_estimated_count()
        if less_than:
            less_than -= 1
        title_item = next(items)
        data_stream = self.stream(items, count=count, less_than=less_than)
        return title_item, data_stream

    def split_by_pos(self, pos: int) -> tuple:
        first_stream, second_stream = self.tee_streams(2)
        return (
            first_stream.take(pos),
            second_stream.skip(pos),
        )

    def split_by_list_pos(self, list_pos: Union[list, tuple]) -> list:
        count_limits = len(list_pos)
        cloned_streams = self.tee_streams(count_limits + 1)
        filtered_streams = list()
        prev_pos = 0
        for n, cur_pos in enumerate(list_pos):
            count_items = cur_pos - prev_pos
            cur_stream = cloned_streams[n].skip(
                prev_pos,
            ).take(
                count_items,
            ).update_meta(
                count=count_items,
            )
            filtered_streams.append(
                cur_stream
            )
            prev_pos = cur_pos
        filtered_streams.append(
            cloned_streams[-1].skip(
                list_pos[-1],
            )
        )
        return filtered_streams

    def split_by_numeric(self, func: Callable, count: int) -> list:
        return [
            f.filter(
                lambda i, n=n: func(i) == n,
            ) for n, f in enumerate(
                self.tee_streams(count),
            )
        ]

    def split_by_boolean(self, func: Callable) -> list:
        return self.split_by_numeric(
            func=lambda f: int(bool(func(f))),
            count=2,
        )

    def split(self, by: Union[int, list, tuple, Callable], count: Count = None) -> Iterable:
        if isinstance(by, int):
            return self.split_by_pos(by)
        elif isinstance(by, (list, tuple)):
            return self.split_by_list_pos(by)
        elif callable(by):
            if count:
                return self.split_by_numeric(by, count)
            else:
                return self.split_by_boolean(by)
        else:
            raise TypeError('split(by): by-argument must be int, list, tuple or function, {} received'.format(type(by)))

    @staticmethod
    def _get_split_items(items: Iterable, step: int):
        output_items = list()
        for n, i in enumerate(items):
            output_items.append(i)
            if n + 1 >= step:
                break
        return output_items

    def split_to_iter_by_step(self, step: int) -> Iterable:
        iterable = self.get_iter()
        items = self._get_split_items(iterable, step=step)
        while items:
            yield self.stream(items, count=len(items))
            items = self._get_split_items(iterable, step=step)

    def _get_filtered_items(self, function: Callable) -> Iterable:
        return filter(function, self.get_items())

    def filter(self, function) -> Native:
        stream = self.stream(
            self._get_filtered_items(function),
            count=None,
            less_than=self.get_estimated_count(),
        )
        return self._assume_native(stream)

    def _get_mapped_items(self, function: Callable, flat: bool = False) -> Iterable:
        if flat:
            for i in self.get_iter():
                yield from function(i)
        else:
            yield from map(function, self.get_items())

    def map(self, function: Callable) -> Native:
        items = self._get_mapped_items(function, flat=False)
        stream = self.stream(items)
        return self._assume_native(stream)

    def flat_map(self, function: Callable) -> Native:
        items = self._get_mapped_items(function, flat=True)
        stream = self.stream(items)
        return self._assume_native(stream)

    def map_side_join(self, right: Native, key: UniKey, how: How = JoinType.Left, right_is_uniq: bool = True) -> Native:
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
        stream = self.stream(
            list(joined_items) if self.is_in_memory() else joined_items,
        ).set_meta(
            **self.get_static_meta()
        )
        return self._assume_native(stream)

    def progress(
            self,
            expected_count: AutoCount = AUTO,
            step: AutoCount = AUTO,
            message: str = 'Progress',
    ) -> Native:
        count = arg.acquire(expected_count, self.get_count()) or self.get_estimated_count()
        logger = self.get_logger()
        if isinstance(logger, ExtLogger):
            items_with_logger = logger.progress(self.get_items(), name=message, count=count, step=step)
        else:
            if logger:
                logger.log(msg=message, level=LoggingLevel.Info)
            items_with_logger = self.get_items()
        stream = self.stream(items_with_logger)
        return self._assume_native(stream)

    def get_dict(self, key: UniKey, value: UniKey) -> dict:
        key_getter = self._get_field_getter(key)
        value_getter = self._get_field_getter(value)
        return {key_getter(i): value_getter(i) for i in self.get_items()}

    def get_demo_example(self, count: int = 3) -> Iterable:
        if isinstance(self, IterableStreamInterface) or hasattr(self, 'tee_stream'):
            yield from self.tee_stream().take(count).get_items()

    def show(self, *args, **kwargs):
        self.log(str(self), end='\n', verbose=True, truncate=False, force=True)
        demo_example = self.get_demo_example(*args, **kwargs)
        if isinstance(demo_example, Iterable):
            demo_example = [str(i) for i in demo_example]
            for example_item in demo_example:
                msg = 'example: {}'.format(example_item)
                self.log(msg=msg, level=LoggingLevel.Info, verbose=False)
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
                    value = self.__getattribute__(name)(*args, **kwargs)
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
        self.log(value, verbose=show)

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
        self.log(value, end='\n', verbose=True)
        return self

    @staticmethod
    def _assume_native(stream) -> Native:
        return stream

    def get_selection_logger(self) -> SelectionLogger:
        if isinstance(self, IterableStreamInterface) or hasattr(self, 'get_context'):
            context = self.get_context()
        else:
            context = None
        if context:
            return context.get_selection_logger()
        else:
            logger = self.get_logger()
            if isinstance(logger, ExtLogger) or hasattr(logger, 'get_selection_logger'):
                return logger.get_selection_logger()
