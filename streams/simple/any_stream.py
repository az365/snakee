from itertools import chain, tee
from datetime import datetime
import inspect
import json

try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as sm
    from utils import (
        arguments as arg,
        mappers as ms,
        selection,
        algo,
    )
    from loggers import logger_classes as log
    from functions import all_functions as fs
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import stream_classes as sm
    from ...utils import (
        arguments as arg,
        mappers as ms,
        selection,
        algo,
    )
    from ...loggers import logger_classes as log
    from ...functions import all_functions as fs


class AnyStream:
    def __init__(
            self,
            data,
            count=None,
            less_than=None,
            source=None,
            context=None,
            max_items_in_memory=sm.MAX_ITEMS_IN_MEMORY,
            tmp_files_template=sm.TMP_FILES_TEMPLATE,
            tmp_files_encoding=sm.TMP_FILES_ENCODING,
    ):
        self.data = data
        if isinstance(data, (list, tuple)):
            self.count = len(data)
        else:
            self.count = count
        self.less_than = less_than or self.count
        self.source = source
        if source:
            self.name = source.get_name()
        else:
            self.name = arg.DEFAULT
        self.max_items_in_memory = max_items_in_memory
        self.tmp_files_template = tmp_files_template
        self.tmp_files_encoding = tmp_files_encoding
        if not context:
            context = sm.get_context()
        self.context = context
        if context is not None:
            self.put_into_context()

    def get_context(self):
        return self.context

    def put_into_context(self, name=arg.DEFAULT):
        assert self.context, 'for put_into_context context must be defined'
        name = arg.undefault(name, self.name)
        if name not in self.context.stream_instances:
            self.context.stream_instances[name] = self

    def get_name(self):
        return self.name

    def set_name(self, name, register=True):
        if register:
            old_name = self.get_name()
            self.context.rename_stream(old_name, name)
        self.name = name
        return self

    def get_logger(self):
        if self.get_context():
            return self.get_context().get_logger()
        else:
            return log.get_logger()

    def get_items(self):
        return self.data

    def get_meta(self):
        meta = self.__dict__.copy()
        meta.pop('data')
        meta.pop('name')
        return meta

    def get_meta_except_count(self):
        meta = self.get_meta()
        meta.pop('count')
        return meta

    def set_meta(self, **meta):
        return self.__class__(
            self.data,
            **meta
        )

    def update_meta(self, **meta):
        props = self.get_meta()
        props.update(meta)
        return self.__class__(
            self.data,
            **props
        )

    def fill_meta(self, check=True, **meta):
        props = self.get_meta()
        if check:
            unsupported = [k for k in meta if k not in props]
            assert not unsupported, 'class {} does not support these properties: {}'.format(
                self.get_stream_type(),
                unsupported,
            )
        for key, value in props.items():
            if value is None or value == arg.DEFAULT:
                props[key] = meta.get(key)
        return self.__class__(
            self.data,
            **props
        )

    def get_class_name(self):
        return self.__class__.__name__

    def get_stream_type(self):
        return sm.StreamType(self.get_class_name())

    @classmethod
    def get_class(cls, other=None):
        if other is None:
            return cls
        elif isinstance(other, (sm.StreamType, str)):
            return sm.StreamType(other).get_class()
        elif inspect.isclass(other):
            return other
        else:
            raise TypeError('"other" parameter must be class or StreamType (got {})'.format(type(other)))

    def get_property(self, name, *args, **kwargs):
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

    def log(self, msg, level=arg.DEFAULT, end=arg.DEFAULT, verbose=True):
        logger = self.get_logger()
        if logger is not None:
            logger.log(
                msg=msg, level=level,
                end=end, verbose=verbose,
            )

    def get_links(self):
        if self.source is not None:
            yield self.source

    def close(self, recursively=False):
        try:
            self.pass_items()
            closed_streams = 1
        except BaseException as e:
            self.log(['Error while trying to close stream:', e], level=log.LoggingLevel.Warning)
            closed_streams = 0
        closed_links = 0
        if recursively:
            for link in self.get_links():
                if hasattr(link, 'close'):
                    closed_links += link.close() or 0
        return closed_streams, closed_links

    @classmethod
    def from_json_file(
            cls,
            filename,
            encoding=None, gzip=False,
            skip_first_line=False, max_count=None,
            check=arg.DEFAULT,
            verbose=False,
    ):
        parsed_stream = sm.LineStream.from_text_file(
            filename,
            encoding=encoding, gzip=gzip,
            skip_first_line=skip_first_line, max_count=max_count,
            check=check,
            verbose=verbose,
        ).parse_json(
            to=cls.__name__,
        )
        return parsed_stream

    @staticmethod
    def is_valid_item(item):
        return True

    @staticmethod
    def valid_items(items, **kwargs):
        return items

    def validated(self, skip_errors=False):
        return self.__class__(
            self.valid_items(self.get_items(), skip_errors=skip_errors),
            **self.get_meta()
        )

    def iterable(self):
        yield from self.get_items()

    def next(self):
        return next(
            self.iterable(),
        )

    def one(self):
        for i in self.get_items():
            return i

    def expected_count(self):
        return self.count

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
            return self.expected_count()

    def estimate_count(self):
        return self.count or self.less_than

    def tee(self, n=2):
        return [
            self.__class__(
                i,
                count=self.count,
            ) for i in tee(
                self.get_items(),
                n,
            )
        ]

    def copy(self):
        if self.is_in_memory():
            copy_data = self.get_list().copy()
        else:
            self.data, copy_data = tee(self.iterable(), 2)
        return self.__class__(
            copy_data,
            **self.get_meta()
        )

    def calc(self, function):
        return function(self.data)

    def lazy_calc(self, function):
        yield from function(self.data)

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

    def apply_to_stream(self, function, *args, **kwargs):
        return function(self, *args, **kwargs)

    def apply(self, function, *args, to_stream=False, **kwargs):
        if to_stream:
            return self.apply_to_stream(function, *args, **kwargs)
        else:
            return self.apply_to_data(function, *args, **kwargs)

    def native_map(self, function):
        return self.__class__(
            map(function, self.get_items()),
            count=self.count,
            less_than=self.count or self.less_than,
        )

    def map_to_any(self, function):
        return AnyStream(
            map(function, self.get_items()),
            count=self.count,
            less_than=self.count or self.less_than,
        )

    def map_to_records(self, function=None):
        def get_record(i):
            if function is None:
                return i if isinstance(i, dict) else dict(item=i)
            else:
                return function(i)
        return sm.RecordStream(
            map(get_record, self.get_items()),
            count=self.count,
            less_than=self.less_than,
            check=True,
        )

    def map(self, function, to=arg.DEFAULT):
        to = arg.undefault(to, self.get_stream_type())
        stream_class = sm.get_class(to)
        new_props_keys = stream_class([]).get_meta().keys()
        props = {k: v for k, v in self.get_meta().items() if k in new_props_keys}
        items = map(function, self.iterable())
        result = stream_class(
            items,
            **props
        )
        if self.is_in_memory():
            return result.to_memory()
        else:
            return result

    def flat_map(self, function, to=arg.DEFAULT):
        def get_items():
            for i in self.iterable():
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

    def filter(self, *functions):
        def filter_function(item):
            for f in functions:
                if not f(item):
                    return False
            return True
        props = self.get_meta_except_count()
        filtered_items = filter(filter_function, self.iterable())
        if self.is_in_memory():
            filtered_items = list(filtered_items)
            props['count'] = len(filtered_items)
        else:
            props['less_than'] = self.count or self.less_than
        return self.__class__(
            filtered_items,
            **props
        )

    def select(self, *columns, **expressions):
        if columns and not expressions:
            return self.to_rows(
                function=lambda i: selection.row_from_any(i, *columns),
            )
        elif expressions and not columns:
            descriptions = selection.flatten_descriptions(**expressions)
            return self.to_records(
                function=lambda i: selection.record_from_any(i, *descriptions),
            )
        else:
            message = 'for AnyStream use either columns (returns RowStream) or expressions (returns RecordStream), not both'
            raise ValueError(message)

    def enumerated_items(self):
        for n, i in enumerate(self.get_items()):
            yield n, i

    def enumerate(self, native=False):
        props = self.get_meta()
        if native:
            target_class = self.__class__
        else:
            target_class = sm.KeyValueStream
            props['secondary'] = sm.StreamType(self.get_class_name())
        return target_class(
            self.enumerated_items(),
            **props
        )

    def progress(self, expected_count=arg.DEFAULT, step=arg.DEFAULT, message='Progress'):
        count = arg.undefault(expected_count, self.count) or self.less_than
        items_with_logger = self.get_logger().progress(self.data, name=message, count=count, step=step)
        return self.__class__(
            items_with_logger,
            **self.get_meta()
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
        old_items = self.iterable()
        new_items = items
        if before:
            chain_records = chain(new_items, old_items)
        else:
            chain_records = chain(old_items, new_items)
        props = self.get_meta_except_count()
        return self.__class__(
            chain_records,
            **props
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
        items = self.iterable()
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
        first_stream, second_stream = self.tee(2)
        return (
            first_stream.take(pos),
            second_stream.skip(pos),
        )

    def split_by_list_pos(self, list_pos):
        count_limits = len(list_pos)
        cloned_streams = self.tee(count_limits + 1)
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
                self.tee(count),
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
        iterable = self.iterable()

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

    def split_to_disk_by_step(
            self,
            step=arg.DEFAULT,
            file_template=arg.DEFAULT, encoding=arg.DEFAULT,
            sort_each_by=None, reverse=False,
            verbose=True,
    ):
        file_template = arg.undefault(file_template, self.tmp_files_template)
        encoding = arg.undefault(encoding, self.tmp_files_encoding)
        result_parts = list()
        for part_no, sm_part in enumerate(self.to_iter().split_to_iter_by_step(step)):
            part_fn = file_template.format(part_no)
            self.log('Sorting part {} and saving into {} ... '.format(part_no, part_fn), verbose=verbose)
            if sort_each_by:
                sm_part = sm_part.memory_sort(
                    key=sort_each_by,
                    reverse=reverse,
                    verbose=verbose,
                )
            self.log('Writing {} ...'.format(part_fn), end='\r', verbose=verbose)
            sm_part = sm_part.to_json().to_text_file(part_fn, encoding=encoding).map_to_any(json.loads)
            result_parts.append(sm_part)
        return result_parts

    def memory_sort(self, key=fs.same(), reverse=False, verbose=False):
        key_function = fs.composite_key(key)
        list_to_sort = self.get_list()
        count = len(list_to_sort)
        self.log('Sorting {} items in memory...'.format(count), end='\r', verbose=verbose)
        sorted_items = sorted(
            list_to_sort,
            key=key_function,
            reverse=reverse,
        )
        self.log('Sorting has been finished.', end='\r', verbose=verbose)
        self.count = len(sorted_items)
        return self.__class__(
            sorted_items,
            **self.get_meta()
        )

    def disk_sort(self, key=fs.same(), reverse=False, step=arg.DEFAULT, verbose=False):
        step = arg.undefault(step, self.max_items_in_memory)
        key_function = fs.composite_key(key)
        stream_parts = self.split_to_disk_by_step(
            step=step,
            sort_each_by=key_function, reverse=reverse,
            verbose=verbose,
        )
        assert stream_parts, 'streams must be non-empty'
        iterables = [f.iterable() for f in stream_parts]
        counts = [f.count for f in stream_parts]
        props = self.get_meta()
        props['count'] = sum(counts)
        self.log('Merging {} parts... '.format(len(iterables)), verbose=verbose)
        return self.__class__(
            algo.merge_iter(iterables, key_function=key_function, reverse=reverse),
            **props
        )

    def sort(self, *keys, reverse=False, step=arg.DEFAULT, verbose=True):
        keys = arg.update(keys)
        step = arg.undefault(step, self.max_items_in_memory)
        if len(keys) == 0:
            key_function = fs.same()
        else:
            key_function = fs.composite_key(keys)
        if self.can_be_in_memory():
            return self.memory_sort(key_function, reverse=reverse, verbose=verbose)
        else:
            return self.disk_sort(key_function, reverse=reverse, step=step, verbose=verbose)

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

    def sorted_join(self, right, key, how='left', sorting_is_reversed=False):
        assert sm.is_stream(right)
        assert how in algo.JOIN_TYPES, 'only {} join types are supported ({} given)'.format(algo.JOIN_TYPES, how)
        keys = arg.update([key])
        joined_items = algo.sorted_join(
            iter_left=self.iterable(),
            iter_right=right.iterable(),
            key_function=fs.composite_key(keys),
            how=how,
            sorting_is_reversed=sorting_is_reversed,
        )
        return self.__class__(
            list(joined_items) if self.is_in_memory() else joined_items,
            **self.get_meta_except_count()
        )

    def join(self, right, key, how='left', reverse=False, verbose=arg.DEFAULT):
        return self.sort(
            key,
            reverse=reverse,
            verbose=verbose,
        ).sorted_join(
            right.sort(
                key,
                reverse=reverse,
                verbose=verbose,
            ),
            key=key, how=how,
            sorting_is_reversed=reverse,
        )

    def get_list(self):
        return list(self.get_items())

    def is_in_memory(self):
        return isinstance(self.data, (list, tuple))

    def can_be_in_memory(self, step=arg.DEFAULT):
        step = arg.undefault(step, self.max_items_in_memory)
        if self.is_in_memory() or step is None:
            return True
        else:
            return self.estimate_count() is not None and self.estimate_count() <= step

    def to_memory(self):
        items_as_list_in_memory = self.get_list()
        props = self.get_meta()
        props['count'] = len(items_as_list_in_memory)
        if 'check' in props:
            props['check'] = False
        return self.__class__(
            items_as_list_in_memory,
            **props
        )

    def to_iter(self):
        return self.__class__(
            self.iterable(),
            **self.get_meta()
        )

    def to_any(self):
        return sm.AnyStream(
            self.get_items(),
            count=self.count,
            less_than=self.less_than,
        )

    def to_lines(self, **kwargs):
        return sm.LineStream(
            self.map_to_any(str).get_items(),
            count=self.count,
            less_than=self.less_than,
            check=True,
        )

    def to_json(self):
        return self.map_to_any(
            json.dumps
        ).to_lines()

    def to_rows(self, *args, **kwargs):
        function = kwargs.pop('function', None)
        if kwargs:
            message = 'to_rows(): kwargs {} are not supported for class {}'.format(kwargs.keys(), self.get_class_name())
            raise ValueError(message)
        if args:
            message = 'to_rows(): positional arguments are not supported for class {}'.format(self.get_class_name())
            raise ValueError(message)
        return sm.RowStream(
            map(function, self.get_items()) if function is not None else self.get_items(),
            count=self.count,
            less_than=self.less_than,
        )

    def to_pairs(self, key=fs.value_by_key(0), value=fs.value_by_key(1)):
        if isinstance(key, (list, tuple)):
            key = fs.composite_key(key)
        if isinstance(value, (list, tuple)):
            value = fs.composite_key(value)
        pairs_data = self.map(
            lambda i: selection.row_from_any(i, key, value),
        ).get_items()
        return sm.KeyValueStream(
            pairs_data,
            count=self.count,
            less_than=self.less_than,
        )

    def to_records(self, **kwargs):
        function = kwargs.get('function')
        return self.map_to_records(function)

    def show(self, count=3):
        self.log([self.get_class_name(), self.get_meta()], end='\n', verbose=True)
        if self.is_in_memory():
            for i in self.get_items()[:count]:
                self.log(i, end='\n', verbose=True)
        else:
            self.log(self.one(), end='\n', verbose=True)

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
