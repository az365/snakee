from itertools import chain, tee
from datetime import datetime
from typing import Optional, Callable, Iterable, Generator, Union
import gc

try:  # Assume we're a sub-module in a package.
    from interfaces import (
        IterableStreamInterface,
        StreamType, LoggingLevel, JoinType, How,
        Stream, Source, ExtLogger, SelectionLogger, Context, Connector, LeafConnector,
        AUTO, Auto, AutoName, AutoCount, Count, OptionalFields, Message, Array, UniKey,
    )
    from streams.abstract.abstract_stream import AbstractStream
    from utils import algo, arguments as arg
    from utils.external import pd, DataFrame, get_use_objects_for_output
    from utils.decorators import deprecated_with_alternative
    from streams import stream_classes as sm
    from loggers import logger_classes as log
    from functions import item_functions as fs
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        IterableStreamInterface,
        StreamType, LoggingLevel, JoinType, How,
        Stream, Source, ExtLogger, SelectionLogger, Context, Connector, LeafConnector,
        AUTO, AutoName, AutoCount, Count, OptionalFields, Message, Array, UniKey,
    )
    from ..abstract.abstract_stream import AbstractStream
    from ...utils import algo, arguments as arg
    from ...utils.external import pd, DataFrame, get_use_objects_for_output
    from ...utils.decorators import deprecated_with_alternative
    from .. import stream_classes as sm
    from ...loggers import logger_classes as log
    from ...functions import item_functions as fs

Native = IterableStreamInterface

DYNAMIC_META_FIELDS = ('count', 'less_than')


class IterableStream(AbstractStream, IterableStreamInterface):
    def __init__(
            self,
            data: Iterable,
            name: AutoName = AUTO,
            source: Source = None, context: Context = None,
            count: Count = None, less_than: Count = None,
            check: bool = False,
            max_items_in_memory: AutoCount = AUTO,
    ):
        self._count = count
        self._less_than = less_than or count
        self.check = check
        self.max_items_in_memory = arg.acquire(max_items_in_memory, sm.MAX_ITEMS_IN_MEMORY)
        super().__init__(
            data=self.get_typing_validated_items(data, context=context) if check else data,
            name=name,
            source=source,
            context=context,
            check=check,
        )

    def get_data(self) -> Iterable:
        data = super().get_data()
        assert isinstance(data, Iterable), 'Expected Iterable, got {} as {}'.format(data, data.__class__.__name__)
        return data

    def get_items(self) -> Iterable:  # list or generator (need for inherited subclasses)
        return self.get_data()

    def get_iter(self) -> Generator:
        yield from self.get_items()

    def __iter__(self):
        return self.get_iter()

    @staticmethod
    def _get_dynamic_meta_fields() -> tuple:
        return DYNAMIC_META_FIELDS

    @classmethod
    def is_valid_item_type(cls, item) -> bool:
        return True

    def is_valid_item(self, item) -> bool:
        return self.is_valid_item_type(item)

    @classmethod
    def get_typing_validated_items(
            cls,
            items: Iterable,
            skip_errors: bool = False,
            context: Context = None,
    ) -> Iterable:
        for i in items:
            if cls.is_valid_item_type(i):
                yield i
            else:
                message = 'get_typing_validated_items() found invalid item {} for {}'.format(i, cls.get_stream_type())
                if skip_errors:
                    if context:
                        context.get_logger().log(message)
                else:
                    raise TypeError(message)

    def get_validated_items(self, items: Iterable, skip_errors: bool = False, context: Context = None) -> Iterable:
        for i in items:
            if self.is_valid_item(i):
                yield i
            else:
                message = 'get_validated_items() found invalid item {} for {}'.format(i, self.get_stream_type())
                if skip_errors:
                    if context:
                        context.get_logger().log(message)
                else:
                    raise TypeError(message)

    def is_in_memory(self) -> bool:
        return False

    def close(self, recursively: bool = False, return_closed_links: bool = False) -> Union[int, tuple]:
        self.set_data([], inplace=True)
        closed_streams = 1
        closed_links = 0
        if recursively:
            for link in self.get_links():
                if hasattr(link, 'close'):
                    closed_links += link.close() or 0
        gc.collect()
        if return_closed_links:
            return closed_streams, closed_links
        else:
            return closed_streams

    def get_expected_count(self) -> Count:
        return self._count

    def set_expected_count(self, count: int) -> Native:
        self._count = count
        return self

    def get_one_item(self):
        for i in self.get_tee_items():
            return i

    @deprecated_with_alternative('get_one_item()')
    def one(self):
        return self.get_one_item()

    def next(self):
        return next(
            self.get_iter(),
        )

    def final_count(self) -> int:
        result = 0
        for _ in self.get_items():
            result += 1
        return result

    def get_count(self, final: bool = False) -> Count:
        if final:
            return self.final_count()
        else:
            return self.get_expected_count()

    def get_estimated_count(self) -> Count:
        return self.get_count() or self._less_than

    def set_estimated_count(self, count: int):
        self._less_than = count

    def get_str_count(self) -> str:
        if self.get_count():
            return '{}'.format(self.get_count())
        elif self.get_estimated_count():
            return '<={}'.format(self.get_estimated_count())
        else:
            return '(unknown count)'

    def get_description(self) -> str:
        return '{} items'.format(self.get_str_count())

    def get_first_items(self, count: int = 1) -> Iterable:
        for n, i in self.get_enumerated_items():
            yield i
            if n + 1 >= count:
                break

    def get_enumerated_items(self) -> Iterable:
        for n, i in enumerate(self.get_items()):
            yield n, i

    def enumerate(self, native: bool = False) -> Union[Native, Stream]:
        props = self.get_meta()
        if native:
            target_class = self.__class__
        else:
            target_class = sm.KeyValueStream
            props['value_stream_type'] = sm.StreamType(self.get_class_name())
        return target_class(
            self.get_enumerated_items(),
            **props
        )

    def take(self, count: Union[int, bool] = 1) -> Native:
        if (count and isinstance(count, bool)) or not arg.is_defined(count):  # True, None, AUTO
            return self
        elif isinstance(count, int):
            if count > 0:
                return self.stream(
                    self.get_first_items(count),
                    count=min(self.get_count(), count) if self.get_count() else None,
                    less_than=min(self.get_estimated_count(), count) if self.get_estimated_count() else count,
                )
            elif count < 0:
                return self.tail(count=count)
            else:  # count in (0, False)
                return self.stream([], count=0)

    def skip(self, count: int = 1) -> Native:
        def skip_items(c):
            for n, i in self.get_enumerated_items():
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
        return self.stream(next_items, count=new_count, less_than=less_than)

    def head(self, count: int = 10) -> Native:
        return self.take(count)  # alias

    def tail(self, count: int = 10) -> Native:
        return self.stream(
            self._get_last_items(count),
        )

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
            self.log(['Error while trying to close stream:', e], level=LoggingLevel.Warning)
        return self

    def tee_streams(self, n: int = 2) -> list:
        return [
            self.stream(
                tee_stream,
            ) for tee_stream in tee(
                self.get_items(),
                n,
            )
        ]

    def get_tee_items(self) -> Iterable:
        two_iterators = tee(self.get_items(), 2)
        self._data, tee_items = two_iterators
        return tee_items

    def tee_stream(self) -> Native:
        return self.stream(
            self.get_tee_items(),
            check=False,
        )

    def stream(self, data: Iterable, ex: OptionalFields = None, **kwargs) -> Native:
        stream = super().stream(data, ex=ex, **kwargs)
        return self._assume_native(stream)

    def copy(self) -> Native:
        return self.tee_stream()

    def add(self, stream_or_items: Union[Stream, Iterable], before: bool = False, **kwargs) -> Native:
        if sm.is_stream(stream_or_items):
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
        return self.stream(
            chain_records,
            count=count,
            less_than=less_than,
        )

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
        data_stream = self.stream(
            items,
            count=count,
            less_than=less_than,
        )
        return (
            title_item,
            data_stream,
        )

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

    def get_filtered_items(self, function: Callable) -> Iterable:
        return filter(function, self.get_items())

    def filter(self, function) -> Native:
        return self.stream(
            filter(function, self.get_iter()),
            count=None,
            less_than=self.get_estimated_count(),
        )

    def get_mapped_items(self, function: Callable, flat: bool = False) -> Iterable:
        if flat:
            for i in self.get_iter():
                yield from function(i)
        else:
            yield from map(function, self.get_items())

    def map(self, function: Callable) -> Native:
        return self.stream(
            self.get_mapped_items(function, flat=False),
        )

    def flat_map(self, function: Callable) -> Native:
        return self.stream(
            self.get_mapped_items(function, flat=True),
        )

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
        items_with_logger = self.get_logger().progress(self.get_data(), name=message, count=count, step=step)
        return self.stream(items_with_logger)

    def get_demo_example(self, count: int = 3) -> Iterable:
        yield from self.tee_stream().take(count).get_items()

    def show(self, *args, **kwargs):
        self.log(str(self), end='\n', verbose=True, truncate=False, force=True)
        demo_example = self.get_demo_example(*args, **kwargs)
        if isinstance(demo_example, Iterable):
            demo_example = [str(i) for i in demo_example]
            for example_item in demo_example:
                self.log(('example:', example_item), verbose=False)
            return '\n'.join(demo_example)
        else:
            return demo_example

    def print(self, stream_function: Union[Callable, str] = 'get_count', *args, **kwargs) -> Native:
        value = self.get_property(stream_function, *args, **kwargs)
        self.log(value, end='\n', verbose=True)
        return self

    def submit(
            self,
            external_object: Union[list, dict, Callable] = print,
            stream_function: Union[Callable, str] = 'count',
            key: Optional[str] = None, show=False,
    ) -> Native:
        value = self.get_property(stream_function)
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

    @staticmethod
    def _assume_native(stream) -> Native:
        return stream

    def set_meta(self, **meta) -> Native:
        stream = super().set_meta(**meta)
        return self._assume_native(stream)

    def get_selection_logger(self) -> SelectionLogger:
        if self.get_context():
            return self.get_context().get_selection_logger()
        else:
            return log.get_selection_logger()

    def get_dataframe(self, columns: Optional[Iterable] = None) -> DataFrame:
        if pd and get_use_objects_for_output():
            if columns:
                dataframe = DataFrame(self.get_data(), columns=columns)
                columns = arg.get_names(columns)
                dataframe = dataframe[columns]
            else:
                dataframe = DataFrame(self.get_data())
            return dataframe
