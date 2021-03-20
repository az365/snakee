from abc import ABC, abstractmethod
from itertools import chain, tee
from typing import Iterable
from datetime import datetime

try:  # Assume we're a sub-module in a package.
    from utils import (
        arguments as arg,
        algo,
    )
    from streams import stream_classes as sm
    from loggers import logger_classes as log
    from functions import item_functions as fs
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import (
        arguments as arg,
        algo,
    )
    from .. import stream_classes as sm
    from ...loggers import logger_classes as log
    from ...functions import item_functions as fs


class IterableStream(sm.AbstractStream, ABC):
    def __init__(
            self,
            data: Iterable,
            name=arg.DEFAULT,
            source=None, context=None,
            count=None, less_than=None,
            check=False,
            max_items_in_memory=arg.DEFAULT,
    ):
        super().__init__(
            data=self.get_validated(data, context=context) if check else data,
            name=name,
            source=source,
            context=context,
        )
        if isinstance(data, (list, tuple)):
            self.count = len(data)
        else:
            self.count = count
        self.less_than = less_than or self.count
        self.check = check
        self.max_items_in_memory = arg.undefault(max_items_in_memory, sm.MAX_ITEMS_IN_MEMORY)

    def get_items(self):
        return self.get_data()

    def get_meta_except_count(self):
        return self.get_meta(
            ex=['count', 'less_than'],
        )

    def get_expected_count(self):
        return self.count

    @staticmethod
    def is_valid_item(item):
        return True

    @classmethod
    def get_validated(cls, items, skip_errors=False, context=None):
        for i in items:
            if cls.is_valid_item(i):
                yield i
            else:
                message = 'get_validated() found invalid item {} for {}'.format(i, cls.get_stream_type())
                if skip_errors:
                    if context:
                        context.get_logger().log(message)
                else:
                    raise TypeError(message)

    def close(self, recursively=False):
        try:
            self.pass_items()
            closed_streams = 1
        except BaseException as e:
            self.log(['Error while trying to close stream:', e], level=loggers.extended_logger_interface.LoggingLevel.Warning)
            closed_streams = 0
        closed_links = 0
        if recursively:
            for link in self.get_links():
                if hasattr(link, 'close'):
                    closed_links += link.close() or 0
        return closed_streams, closed_links

    def get_iter(self):
        yield from self.get_items()

    def next(self):
        return next(
            self.get_iter(),
        )

    def one(self):
        for i in self.get_tee_items():
            return i

    def final_count(self):
        result = 0
        for _ in self.get_items():
            result += 1
        return result

    def get_count(self, in_memory=False, final=False):
        if in_memory:
            self.data = self.get_list()
            self.count = len(self.data)
            return self.count
        elif final:
            return self.final_count()
        else:
            return self.get_expected_count()

    def get_estimated_count(self):
        return self.count or self.less_than

    def get_str_count(self):
        if self.count:
            return '{}'.format(self.count)
        elif self.less_than:
            return '<={}'.format(self.less_than)
        else:
            return '(unknown count)'

    def get_description(self):
        return '{} items'.format(self.get_str_count())

    def enumerated_items(self):
        for n, i in enumerate(self.get_items()):
            yield n, i

    def enumerate(self, native=False):
        props = self.get_meta()
        if native:
            target_class = self.__class__
        else:
            target_class = sm.KeyValueStream
            props['value_stream_type'] = sm.StreamType(self.get_class_name())
        return target_class(
            self.enumerated_items(),
            **props
        )

    def take(self, max_count=1):
        def take_items(m):
            for n, i in self.enumerated_items():
                yield i
                if n + 1 >= m:
                    break
        props = self.get_meta()
        props['count'] = min(self.count, max_count) if self.count else None
        return self.__class__(
            take_items(max_count),
            **props
        )

    def skip(self, count=1):
        def skip_items(c):
            for n, i in self.enumerated_items():
                if n >= c:
                    yield i
        if self.count and count >= self.count:
            next_items = []
        else:
            next_items = self.get_items()[count:] if self.is_in_memory() else skip_items(count)
        props = self.get_meta()
        if self.count:
            props['count'] = self.count - count
        elif self.less_than:
            props['less_than'] = self.less_than - count
        return self.__class__(
            next_items,
            **props
        )

    def pass_items(self):
        for _ in self.get_items():
            pass

    def get_tee(self, n=2):
        return [
            self.__class__(
                i,
                **self.get_meta()
            ) for i in tee(
                self.get_items(),
                n,
            )
        ]

    def get_tee_items(self):
        two_iterators = tee(self.get_items(), 2)
        self.data, tee_items = two_iterators
        return tee_items

    def get_tee_stream(self):
        return self.stream(
            self.get_tee_items(),
        )

    def add(self, stream_or_items, before=False, **kwargs):
        if sm.is_stream(stream_or_items):
            return self.add_stream(
                stream_or_items,
                before=before,
            )
        else:
            return self.add_items(
                stream_or_items,
                before=before,
            )

    def add_items(self, items, before=False):
        old_items = self.get_items()
        new_items = items
        if before:
            chain_records = chain(new_items, old_items)
        else:
            chain_records = chain(old_items, new_items)
        if isinstance(items, (list, tuple)):
            count = self.count
            if count:
                count += len(items)
            less_than = self.less_than
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

    def add_stream(self, stream, before=False):
        old_count = self.count
        new_count = stream.count
        if old_count is not None and new_count is not None:
            total_count = new_count + old_count
        else:
            total_count = None
        return self.add_items(
            stream.get_items(),
            before=before,
        ).update_meta(
            count=total_count,
        )

    def count_to_items(self):
        return self.add_items(
            [self.count],
            before=True,
        )

    def separate_count(self):
        return (
            self.count,
            self,
        )

    def separate_first(self):
        items = self.get_iter()
        props = self.get_meta()
        if props.get('count'):
            props['count'] -= 1
        title_item = next(items)
        data_stream = self.__class__(
            items,
            **props
        )
        return (
            title_item,
            data_stream,
        )

    def split_by_pos(self, pos):
        first_stream, second_stream = self.get_tee(2)
        return (
            first_stream.take(pos),
            second_stream.skip(pos),
        )

    def split_by_list_pos(self, list_pos):
        count_limits = len(list_pos)
        cloned_streams = self.get_tee(count_limits + 1)
        filtered_streams = list()
        prev_pos = 0
        for n, cur_pos in enumerate(list_pos):
            count_items = cur_pos - prev_pos
            filtered_streams.append(
                cloned_streams[n].skip(
                    prev_pos,
                ).take(
                    count_items,
                ).update_meta(
                    count=count_items,
                )
            )
            prev_pos = cur_pos
        filtered_streams.append(
            cloned_streams[-1].skip(
                list_pos[-1],
            )
        )
        return filtered_streams

    def split_by_numeric(self, func, count):
        return [
            f.filter(
                lambda i, n=n: func(i) == n,
            ) for n, f in enumerate(
                self.get_tee(count),
            )
        ]

    def split_by_boolean(self, func):
        return self.split_by_numeric(
            func=lambda f: int(bool(func(f))),
            count=2,
        )

    def split(self, by, count=None):
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

    def split_to_iter_by_step(self, step):
        iterable = self.get_iter()

        def take_items():
            output_items = list()
            for n, i in enumerate(iterable):
                output_items.append(i)
                if n + 1 >= step:
                    break
            return output_items
        items = take_items()
        props = self.get_meta()
        while items:
            props['count'] = len(items)
            yield self.__class__(
                items,
                **props
            )
            items = take_items()

    def flat_map(self, function, to=arg.DEFAULT):
        def get_items():
            for i in self.get_iter():
                yield from function(i)
        to = arg.undefault(to, self.get_stream_type())
        stream_class = sm.get_class(to)
        new_props_keys = stream_class([]).get_meta().keys()
        props = {k: v for k, v in self.get_meta().items() if k in new_props_keys}
        props.pop('count')
        return stream_class(
            get_items(),
            **props
        )

    def map_side_join(self, right, key, how='left', right_is_uniq=True):
        assert sm.is_stream(right)
        assert how in algo.JOIN_TYPES, 'only {} join types are supported ({} given)'.format(algo.JOIN_TYPES, how)
        keys = arg.update([key])
        joined_items = algo.map_side_join(
            iter_left=self.get_items(),
            iter_right=right.get_items(),
            key_function=fs.composite_key(keys),
            how=how,
            uniq_right=right_is_uniq,
        )
        return self.__class__(
            list(joined_items) if self.is_in_memory() else joined_items,
            **self.get_meta_except_count()
        )

    def calc(self, function):
        return function(self.get_data())

    def lazy_calc(self, function):
        yield from function(self.get_data())

    def apply_to_data(self, function, to=arg.DEFAULT, save_count=False, lazy=True):
        stream_type = arg.undefault(to, self.get_stream_type())
        target_class = sm.get_class(stream_type)
        if to == arg.DEFAULT:
            props = self.get_meta() if save_count else self.get_meta_except_count()
        else:
            props = dict(count=self.count) if save_count else dict()
        return target_class(
            self.lazy_calc(function) if lazy else self.calc(function),
            **props
        )

    def progress(self, expected_count=arg.DEFAULT, step=arg.DEFAULT, message='Progress'):
        count = arg.undefault(expected_count, self.count) or self.less_than
        items_with_logger = self.get_logger().progress(self.data, name=message, count=count, step=step)
        return self.__class__(
            items_with_logger,
            **self.get_meta()
        )

    def get_demo_example(self, count=3):
        yield from self.get_tee_stream().take(count).get_items()

    def show(self, **kwargs):
        self.log(str(self), end='\n', verbose=True, force=True)
        demo_example = [str(i) for i in self.get_demo_example(**kwargs)]
        for example_item in self.get_demo_example(**kwargs):
            self.log(('example:', example_item), verbose=False)
        return '\n'.join(demo_example)

    def print(self, stream_function='count', *args, **kwargs):
        value = self.get_property(stream_function, *args, **kwargs)
        self.log(value, end='\n', verbose=True)
        return self

    def submit(self, external_object=print, stream_function='count', key=None, show=False):
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

    def get_selection_logger(self):
        if self.get_context():
            return self.get_context().get_selection_logger()
        else:
            return log.get_selection_logger()

