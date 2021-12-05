from typing import Optional, Callable, Iterable, Union
from itertools import tee
import gc

try:  # Assume we're a sub-module in a package.
    from utils import algo, arguments as arg
    from utils.external import pd, DataFrame, get_use_objects_for_output
    from utils.decorators import deprecated_with_alternative
    from interfaces import (
        IterableStreamInterface,
        StreamType, LoggingLevel, JoinType, How,
        Stream, Source, ExtLogger, SelectionLogger, Context, Connector, LeafConnector,
        AUTO, Auto, AutoName, AutoCount, Count, OptionalFields, Message, Array, UniKey,
    )
    from streams.mixin.iterable_mixin import IterableStreamMixin
    from streams.abstract.abstract_stream import AbstractStream
    from streams import stream_classes as sm
    from functions.secondary import item_functions as fs
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import algo, arguments as arg
    from ...utils.external import pd, DataFrame, get_use_objects_for_output
    from ...utils.decorators import deprecated_with_alternative
    from ...interfaces import (
        IterableStreamInterface,
        StreamType, LoggingLevel, JoinType, How,
        Stream, Source, ExtLogger, SelectionLogger, Context, Connector, LeafConnector,
        AUTO, AutoName, AutoCount, Count, OptionalFields, Message, Array, UniKey,
    )
    from ..mixin.iterable_mixin import IterableStreamMixin
    from .abstract_stream import AbstractStream
    from .. import stream_classes as sm
    from ...functions.secondary import item_functions as fs

Native = IterableStreamInterface

DYNAMIC_META_FIELDS = ('count', 'less_than')


class IterableStream(AbstractStream, IterableStreamMixin):
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
            data=self._get_typing_validated_items(data, context=context) if check else data,
            name=name,
            source=source,
            context=context,
            check=check,
        )

    def get_stream_data(self) -> Iterable:
        data = super().get_data()
        assert isinstance(data, Iterable), 'Expected Iterable, got {} as {}'.format(data, data.__class__.__name__)
        return data

    def get_data(self) -> Iterable:
        return self.get_stream_data()

    def get_items(self) -> Iterable:  # list or generator (need for inherited subclasses)
        return self.get_stream_data()

    def _get_tee_items(self) -> Iterable:
        two_iterators = tee(self.get_items(), 2)
        data_items, tee_items = two_iterators
        self.set_data(data_items, inplace=True)
        return tee_items

    def tee_stream(self) -> Native:
        stream = self.stream(self._get_tee_items(), check=False)
        return self._assume_native(stream)

    def copy(self) -> Native:
        return self.tee_stream()

    def set_meta(self, inplace: bool = False, **meta) -> Optional[Native]:
        stream = super().set_meta(**meta, inplace=inplace)
        if stream:
            return self._assume_native(stream)

    @staticmethod
    def _get_dynamic_meta_fields() -> tuple:
        return DYNAMIC_META_FIELDS

    @classmethod
    def is_valid_item_type(cls, item) -> bool:
        return True

    def _is_valid_item(self, item) -> bool:
        return self.is_valid_item_type(item)

    @classmethod
    def _get_typing_validated_items(
            cls,
            items: Iterable,
            skip_errors: bool = False,
            context: Context = None,
    ) -> Iterable:
        for i in items:
            if cls.is_valid_item_type(i):
                yield i
            else:
                message = '_get_typing_validated_items() found invalid item {} for {}'.format(i, cls.get_stream_type())
                if skip_errors:
                    if context:
                        context.get_logger().log(msg=message, level=LoggingLevel.Warning)
                else:
                    raise TypeError(message)

    def _get_validated_items(self, items: Iterable, skip_errors: bool = False, context: Context = None) -> Iterable:
        for i in items:
            if self._is_valid_item(i):
                yield i
            else:
                message = '_get_validated_items() found invalid item {} for {}'.format(i, self.get_stream_type())
                if skip_errors:
                    if context:
                        context.get_logger().log(msg=message, level=LoggingLevel.Warning)
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

    def set_count(self, count: int, inplace: bool) -> Optional[Native]:
        if inplace:
            self._count = count
        else:
            return self.make_new(count=count)

    def get_less_than(self) -> Count:
        return self._less_than

    def set_less_than(self, count: int, inplace: bool) -> Optional[Native]:
        if inplace:
            self._less_than = count
        else:
            return self.make_new(less_than=count)

    def get_estimated_count(self) -> Count:
        return self.get_count() or self.get_less_than()

    def set_estimated_count(self, count: int, inplace: bool = True) -> Optional[Native]:
        return self.set_less_than(count, inplace=inplace)

    @deprecated_with_alternative('_get_enumerated_items()')
    def get_enumerated_items(self) -> Iterable:
        return self._get_enumerated_items()

    @deprecated_with_alternative('_get_filtered_items()')
    def get_filtered_items(self, function: Callable) -> Iterable:
        return self._get_filtered_items(function)

    @deprecated_with_alternative('_get_mapped_items()')
    def get_mapped_items(self, function: Callable, flat: bool = False) -> Iterable:
        return self._get_mapped_items(function, flat=flat)

    @deprecated_with_alternative('get_one_item()')
    def one(self):
        return self.get_one_item()

    def get_one_item(self):
        for i in self._get_tee_items():
            return i

    def next(self):
        return next(self.get_iter())

    def get_demo_example(self, count: int = 3) -> Iterable:
        yield from self.tee_stream().take(count).get_items()
