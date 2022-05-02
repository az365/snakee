from typing import Optional, Callable, Iterable, Sized, Union
import gc

try:  # Assume we're a submodule in a package.
    from interfaces import (
        StreamType, LoggingLevel, JoinType, How,
        Stream, Source, ExtLogger, SelectionLogger, Context, Connector, LeafConnector,
        AUTO, Auto, AutoName, AutoCount, Count, OptionalFields, Message, Array, UniKey,
    )
    from base.mixin.iter_data_mixin import IterDataMixin, IterableInterface
    from utils import algo
    from utils.external import pd, DataFrame, get_use_objects_for_output
    from utils.decorators import deprecated_with_alternative
    from functions.secondary import item_functions as fs
    from streams.abstract.abstract_stream import AbstractStream
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        StreamType, LoggingLevel, JoinType, How,
        Stream, Source, ExtLogger, SelectionLogger, Context, Connector, LeafConnector,
        AUTO, Auto, AutoName, AutoCount, Count, OptionalFields, Message, Array, UniKey,
    )
    from ...base.mixin.iter_data_mixin import IterDataMixin, IterableInterface
    from ...utils import algo
    from ...utils.external import pd, DataFrame, get_use_objects_for_output
    from ...utils.decorators import deprecated_with_alternative
    from ...functions.secondary import item_functions as fs
    from .abstract_stream import AbstractStream

Native = Union[AbstractStream, IterDataMixin, IterableInterface]

DYNAMIC_META_FIELDS = ('count', 'less_than')
MAX_ITEMS_IN_MEMORY = 5000000


class IterableStream(AbstractStream, IterDataMixin):
    def __init__(
            self,
            data: Iterable,
            name: AutoName = AUTO,
            caption: str = '',
            source: Source = None, context: Context = None,
            count: Count = None, less_than: Count = None,
            check: bool = False,
            max_items_in_memory: AutoCount = AUTO,
    ):
        self._count = count
        self._less_than = less_than or count
        self.check = check
        self.max_items_in_memory = Auto.acquire(max_items_in_memory, MAX_ITEMS_IN_MEMORY)
        if check:
            data = self._get_typing_validated_items(data, context=context)
        super().__init__(
            data=data, check=False,
            name=name, caption=caption,
            source=source, context=context,
        )

    def get_stream_data(self) -> Iterable:
        data = super().get_data()
        assert isinstance(data, Iterable), 'Expected Iterable, got {} as {}'.format(data, data.__class__.__name__)
        return data

    def get_data(self) -> Iterable:
        return self.get_stream_data()

    def get_items(self) -> Iterable:  # list or generator (need for inherited subclasses)
        return self.get_stream_data()

    @deprecated_with_alternative('IterDataMixin.copy()')
    def tee_stream(self) -> Native:
        stream = self.copy()
        return self._assume_native(stream)

    @deprecated_with_alternative('IterDataMixin.get_tee_clones()')
    def tee_streams(self, n: int = 2) -> list:
        return self.get_tee_clones(n)

    def set_meta(self, inplace: bool = False, **meta) -> Optional[Native]:
        stream = super().set_meta(**meta, inplace=inplace)
        if stream is not None:
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

    def set_expected_count(self, count: int) -> Native:
        self._count = count
        return self

    def get_expected_count(self) -> Count:
        return self._count

    def get_str_count(self, default: str = '(unknown count)') -> str:
        if self.get_count():
            return '{}'.format(self.get_count())
        elif self.get_estimated_count():
            return '<={}'.format(self.get_estimated_count())
        else:
            return default

    def enumerate(self, native: bool = False) -> Union[Native, Stream]:
        props = self.get_meta()
        if native:
            target_class = self.__class__
        else:
            target_class = StreamType.KeyValueStream.get_class()
            props['value_stream_type'] = self.get_stream_type()
        props = self._get_safe_meta(**props)
        return target_class(self._get_enumerated_items(), **props)

    def take(self, count: Union[int, bool] = 1, inplace: bool = False) -> Native:
        return self._assume_native(super().take(count, inplace=inplace))

    def skip(self, count: int = 1, inplace: bool = False) -> Native:
        return self._assume_native(super().skip(count, inplace=inplace))

    def one(self, use_tee: bool = True) -> Native:
        if use_tee:
            item = self.get_one_item()
            stream = self.stream([item])
            return self._assume_native(stream)
        else:
            return self.take(1, inplace=False)

    def get_one_item(self):
        for i in self._get_tee_items():
            return i

    def next(self):
        return next(self.get_iter())

    def pass_items(self) -> Native:
        try:
            super().pass_items()
        except BaseException as e:
            msg = 'Error while trying to close stream: {}'.format(e)
            self.log(msg=msg, level=LoggingLevel.Warning)
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
            stream = self.set_props(count=count, inplace=False)
            return self._assume_native(stream)

    def get_less_than(self) -> Count:
        return self._less_than

    def set_less_than(self, count: int, inplace: bool) -> Optional[Native]:
        if inplace:
            self._less_than = count
        else:
            stream = self.make_new(less_than=count)
            return self._assume_native(stream)

    def get_estimated_count(self) -> Count:
        return self.get_count() or self.get_less_than()

    def set_estimated_count(self, count: int, inplace: bool = True) -> Optional[Native]:
        return self.set_less_than(count, inplace=inplace)

    def add_items(self, items: Iterable, before: bool = False, inplace: bool = False) -> Optional[Native]:
        stream = super().add_items(items, before=before, inplace=inplace)  # IterDataMixin
        estimated_count = self.get_estimated_count()
        if isinstance(items, Sized) and estimated_count is not None and not stream.get_count():
            added_count = len(items)
            exact_count = self.get_count()
            if exact_count:
                exact_count += added_count
            if estimated_count:
                estimated_count += added_count
            if inplace:
                self.update_meta(count=exact_count, less_than=estimated_count, inplace=True)
            else:
                stream = stream.update_meta(count=exact_count, less_than=estimated_count)
        return self._assume_native(stream)

    def add_stream(self, stream: Native, before: bool = False) -> Native:
        old_count = self.get_count()
        new_count = stream.get_count()
        if old_count is not None and new_count is not None:
            total_count = new_count + old_count
        else:
            total_count = None
        stream = self.add_items(stream.get_items(), before=before).update_meta(count=total_count)
        return self._assume_native(stream)

    def count_to_items(self) -> Native:
        return self.add_items(
            [self.get_count()],
            before=True,
        )

    def separate_count(self) -> tuple:
        return self.get_count(), self

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

    def filter(self, function: Callable, inplace: bool = False) -> Optional[Native]:
        return super().filter(
            function,
            inplace=inplace,
        ).update_meta(
            count=None,
            less_than=self.get_estimated_count(),
        )

    def map_side_join(
            self,
            right: Native,
            key: UniKey,
            how: How = JoinType.Left,
            right_is_uniq: bool = True,
            inplace: bool = False,
    ) -> Native:
        stream = super().map_side_join(right, key=key, how=how, right_is_uniq=right_is_uniq, inplace=inplace)
        meta = self.get_static_meta()
        if inplace:
            self.set_meta(**meta, inplace=False)
        else:
            stream = stream.set_meta(**meta, inplace=False)
            return self._assume_native(stream)

    def progress(
            self,
            expected_count: AutoCount = AUTO,
            step: AutoCount = AUTO,
            message: str = 'Progress',
    ) -> Native:
        count = Auto.acquire(expected_count, self.get_count()) or self.get_estimated_count()
        logger = self.get_logger()
        if isinstance(logger, ExtLogger):
            items_with_logger = logger.progress(self.get_items(), name=message, count=count, step=step)
        else:
            if logger:
                logger.log(msg=message, level=LoggingLevel.Info)
            items_with_logger = self.get_items()
        stream = self.stream(items_with_logger)
        return self._assume_native(stream)

    def get_demo_example(self, count: int = 10) -> Iterable:
        stream = self.copy().take(count)
        assert isinstance(stream, AbstractStream)
        yield from stream.get_items()

    def get_selection_logger(self) -> SelectionLogger:
        if hasattr(self, 'get_context'):
            context = self.get_context()
        else:
            context = None
        if context:
            return context.get_selection_logger()
        else:
            logger = self.get_logger()
            if isinstance(logger, ExtLogger) or hasattr(logger, 'get_selection_logger'):
                return logger.get_selection_logger()

    @staticmethod
    def _assume_native(stream) -> Native:
        return stream
