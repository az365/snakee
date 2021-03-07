from abc import ABC, abstractmethod
from itertools import chain, tee
import json

try:  # Assume we're a sub-module in a package.
    from utils import (
        arguments as arg,
        algo,
    )
    from streams import stream_classes as sm
    from functions import item_functions as fs
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import (
        arguments as arg,
        algo,
    )
    from .. import stream_classes as sm
    from ...functions import item_functions as fs


class LocalStream(sm.IterableStream, ABC):
    def __init__(
            self,
            data,
            name=arg.DEFAULT,
            count=None,
            less_than=None,
            source=None,
            context=None,
            max_items_in_memory=sm.MAX_ITEMS_IN_MEMORY,
            tmp_files_template=sm.TMP_FILES_TEMPLATE,
            tmp_files_encoding=sm.TMP_FILES_ENCODING,
    ):
        super().__init__(data=data, name=name, source=source, context=context)
        if isinstance(data, (list, tuple)):
            self.count = len(data)
        else:
            self.count = count
        self.less_than = less_than or self.count
        self.max_items_in_memory = max_items_in_memory
        self.tmp_files_template = tmp_files_template
        self.tmp_files_encoding = tmp_files_encoding

    def get_list(self):
        return list(self.get_items())

    def is_in_memory(self):
        return isinstance(self.data, (list, tuple))

    def to_iter(self):
        return self.__class__(
            self.iterable(),
            **self.get_meta()
        )

    def can_be_in_memory(self, step=arg.DEFAULT):
        step = arg.undefault(step, self.max_items_in_memory)
        if self.is_in_memory() or step is None:
            return True
        else:
            return self.get_estimated_count() is not None and self.get_estimated_count() <= step

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

    def collect(self):
        return self.to_memory()

    def copy(self):
        if self.is_in_memory():
            copy_data = self.get_list().copy()
        else:
            self.data, copy_data = tee(self.iterable(), 2)
        return self.__class__(
            copy_data,
            **self.get_meta()
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

    def get_str_count(self):
        if self.is_in_memory():
            return 'in memory {}'.format(self.get_count())
        else:
            return super().get_str_count()  # IterableStream.get_str_count()

    def get_description(self):
        return '{} items with meta {}'.format(self.get_str_count(), self.get_meta())

    def get_demo_example(self, count=3):
        if self.is_in_memory():
            for i in self.get_items()[:count]:
                yield i
        else:
            yield self.one()
