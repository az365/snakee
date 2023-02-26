from typing import Optional, Callable, Iterable, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        LocalStreamInterface, RegularStreamInterface, ContextInterface, ConnectorInterface, TemporaryFilesMaskInterface,
        ContentType, ItemType, StreamType, StreamItemType, JoinType, How,
        Context, Connector, Array, Count, Name, FieldID, UniKey, OptionalFields,
    )
    from base.classes.auto import Auto
    from base.functions.arguments import update, get_optional_len, is_in_memory
    from functions.secondary import basic_functions as bf, item_functions as fs
    from utils import algo
    from utils.decorators import deprecated_with_alternative
    from streams.abstract.abstract_stream import DEFAULT_EXAMPLE_COUNT
    from streams.abstract.iterable_stream import IterableStream, MAX_ITEMS_IN_MEMORY
    from streams import stream_classes as sm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        LocalStreamInterface, RegularStreamInterface, ContextInterface, ConnectorInterface, TemporaryFilesMaskInterface,
        ContentType, ItemType, StreamType, StreamItemType, JoinType, How,
        Context, Connector, Array, Count, Name, FieldID, UniKey, OptionalFields,
    )
    from ...base.classes.auto import Auto
    from ...base.functions.arguments import update, get_optional_len, is_in_memory
    from ...functions.secondary import basic_functions as bf, item_functions as fs
    from ...utils import algo
    from ...utils.decorators import deprecated_with_alternative
    from .abstract_stream import DEFAULT_EXAMPLE_COUNT
    from .iterable_stream import IterableStream, MAX_ITEMS_IN_MEMORY
    from .. import stream_classes as sm

Native = LocalStreamInterface


class LocalStream(IterableStream, LocalStreamInterface):
    def __init__(
            self,
            data: Iterable,
            name: Optional[Name] = None,
            caption: str = '',
            count: Count = None,
            less_than: Count = None,
            source: Connector = None,
            context: Context = None,
            max_items_in_memory: Count = None,
            tmp_files: Optional[TemporaryFilesMaskInterface] = None,
            check: bool = False,
    ):
        count = get_optional_len(data, count)
        if count:
            if Auto.is_defined(count) and not Auto.is_defined(less_than):
                less_than = count
        self._tmp_files = None
        super().__init__(
            data=data, check=check,
            name=name, caption=caption,
            source=source, context=context,
            count=count, less_than=less_than,
            max_items_in_memory=max_items_in_memory,
        )
        if not Auto.is_defined(tmp_files):
            tmp_files = sm.get_tmp_mask(self.get_name())
        self._tmp_files = tmp_files

    def get_limit_items_in_memory(self) -> int:
        return self.max_items_in_memory

    def set_limit_items_in_memory(self, count: Count, inplace: bool) -> Optional[Native]:
        if inplace:
            self.limit_items_in_memory(count)
        else:
            stream = self.make_new(self.get_data())
            assert isinstance(stream, LocalStream)
            return stream.limit_items_in_memory(count)

    def limit_items_in_memory(self, count: Count) -> Native:
        if not Auto.is_defined(count):
            count = MAX_ITEMS_IN_MEMORY
        self.max_items_in_memory = count
        return self

    def get_list(self, inplace: bool = True) -> list:
        if inplace:
            data = list(self.get_data())
            self.set_data(data, inplace=True)
        else:
            data = list(self.get_items())
        return data

    def is_file(self) -> bool:
        if hasattr(self, 'is_leaf'):
            return self.is_leaf()
        else:
            return False

    def apply_to_data(self, function: Callable, dynamic: bool = False, *args, **kwargs):
        return self.stream(  # can be a file
            self._get_calc(function, *args, **kwargs),
            ex=self._get_dynamic_meta_fields() if dynamic else None,
        )

    def close(
            self,
            recursively: bool = False, return_closed_links: bool = False, remove_tmp_files: bool = True,
    ) -> Union[int, tuple]:
        result = super().close(recursively=recursively, return_closed_links=return_closed_links)
        if remove_tmp_files:
            self.remove_tmp_files()
        return result

    def can_be_in_memory(self, step: Count = None) -> bool:
        if not Auto.is_defined(step):
            step = self.get_limit_items_in_memory()
        if self.is_in_memory() or step is None:
            return True
        else:
            count = self.get_estimated_count()
            if count is None:
                return False
            else:
                return count <= step

    def is_in_memory(self) -> bool:
        if self.is_file():
            return False
        else:
            return is_in_memory(self.get_data())

    def to_memory(self) -> Native:
        items_as_list_in_memory = self.get_list()
        count = len(items_as_list_in_memory)
        self.set_items(items_as_list_in_memory, count=count, inplace=True)
        return self

    def to_iter(self) -> Native:
        stream = self.stream(self.get_iter())
        return self._assume_native(stream)

    def collect(self, inplace: bool = False, log: Optional[bool] = None) -> Native:
        if inplace:
            self._collect_inplace(log=log)
            return self
        else:
            return self.to_memory()

    def _collect_inplace(self, log: Optional[bool] = None) -> None:
        estimated_count = self.get_estimated_count()
        if Auto.is_defined(estimated_count):
            log = Auto.acquire(log, estimated_count > self.get_limit_items_in_memory())
        if log and estimated_count:
            self.log(f'Trying to collect {estimated_count} items into memory from {repr(self)}...')
        self.set_data(self.get_list(), inplace=True)
        self.update_count(force=False)
        if log:
            self.log(f'Collected {estimated_count} items into memory from {repr(self)}...')

    def assert_not_empty(self, message: Optional[str] = None, skip_error: bool = False) -> Native:
        if self.is_iter():
            self._collect_inplace()
        if not Auto.is_defined(message):
            message = 'Empty stream: {}'
        if '{}' in message:
            message = message.format(self)
        if self.is_empty():
            logger = self.get_logger()
            logger.warning(msg=message, stacklevel=2)
            if not skip_error:
                raise AssertionError(message)
        return self

    def tail(self, count: int = DEFAULT_EXAMPLE_COUNT, inplace: bool = False) -> Optional[Native]:
        total_count = self.get_count()
        if total_count:
            stream = self.skip(total_count - count, inplace=inplace)
        else:
            stream = super().tail(count, inplace=inplace)
        return self._assume_native(stream)

    def copy(self) -> Native:
        if self.is_in_memory():
            items = self.get_list().copy()
            stream = self.stream(items)
            return self._assume_native(stream)
        else:  # is iterable generator
            return self._assume_native(super().copy())

    def _get_tee_items(self, mem_copy: bool = False) -> Iterable:
        if self.is_in_memory():
            return self.copy().get_items() if mem_copy else self.get_items()
        else:
            return super()._get_tee_items()

    @deprecated_with_alternative('copy()')
    def get_tee_items(self, mem_copy: bool = False) -> Iterable:
        return self._get_tee_items(mem_copy=mem_copy)

    def map_to_type(self, function: Callable, stream_type: StreamItemType = ItemType.Auto, **kwargs) -> Native:
        if not Auto.is_defined(stream_type):
            stream_type = self.get_stream_type()
        data = map(function, self.get_iter())
        stream = self.stream(data, stream_type=stream_type, **kwargs)
        stream = self._assume_native(stream)
        if self.is_in_memory():
            stream = stream.to_memory()
        return stream

    def map(self, function: Callable, inplace: bool = False) -> Native:
        stream = super().map(function, inplace=inplace) or self
        if self.is_in_memory() and hasattr(stream, 'to_memory'):
            stream = stream.collect(inplace=inplace) or self
        return self._assume_native(stream)

    def filter(self, function: Callable, inplace: bool = False) -> Optional[Native]:
        filtered_items = self._get_filtered_items(function)
        if self.is_in_memory():
            filtered_items = list(filtered_items)
            count = len(filtered_items)
            return self.set_items(filtered_items, count=count, inplace=inplace)
        else:
            stream = super().filter(function, inplace=inplace)
            return self._assume_native(stream)

    def split(self, by: Union[int, list, tuple, Callable], count: Count = None) -> Iterable:
        for stream in super().split(by=by, count=count):
            if self.is_in_memory():
                stream = stream.to_memory()
            yield stream

    def memory_sort(self, key: UniKey = fs.same(), reverse: bool = False, verbose: Optional[bool] = False) -> Native:
        key_function = fs.composite_key(key)
        list_to_sort = self.get_list()
        count = len(list_to_sort)
        self.log(f'Sorting {count} items in memory...', end='\r', verbose=verbose)
        sorted_items = sorted(list_to_sort, key=key_function, reverse=reverse)
        self.log(f'Sorting {count} items has been finished.', end='\r', verbose=verbose)
        self._count = len(sorted_items)
        stream = self.stream(sorted_items)
        return self._assume_native(stream)

    def disk_sort(
            self,
            key: UniKey = fs.same(),
            reverse: bool = False,
            step: Count = None,
            verbose: Optional[bool] = False,
    ) -> Native:
        if not Auto.is_defined(step):
            step = self.get_limit_items_in_memory()
        key_function = fs.composite_key(key)
        stream_parts = self.split_to_disk_by_step(step, sort_each_by=key_function, reverse=reverse, verbose=verbose)
        assert stream_parts, 'streams must be non-empty'
        iterables = [f.get_iter() for f in stream_parts]
        parts_count = len(iterables)
        item_counts = [f.get_count() or 0 for f in stream_parts]
        self.log(f'Merging {parts_count} parts... ', verbose=verbose)
        items = algo.merge_iter(
            iterables,
            key_function=key_function,
            reverse=reverse,
            post_action=self.get_tmp_files().remove_all,
        )
        stream = self.stream(items, count=sum(item_counts))
        return self._assume_native(stream)

    def sort(self, *keys, reverse: bool = False, step: Count = None, verbose: Optional[bool] = True) -> Native:
        keys = update(keys)
        if not Auto.is_defined(step):
            step = self.get_limit_items_in_memory()
        if len(keys) == 0:
            key_function = fs.same()
        else:
            key_function = fs.composite_key(keys)
        if self.can_be_in_memory(step=step) or step is None:
            stream = self.memory_sort(key_function, reverse=reverse, verbose=verbose)
        else:
            stream = self.disk_sort(key_function, reverse=reverse, step=step, verbose=verbose)
        return self._assume_native(stream)

    def sorted_join(
            self,
            right: Native,
            key: UniKey,
            how: How = JoinType.Left,
            sorting_is_reversed: bool = False,
            key_function: Optional[Callable] = None,
            merge_function: Callable = fs.merge_two_items(),
    ) -> Native:
        keys = update([key])
        if not Auto.is_defined(key_function):
            key_function = fs.composite_key(keys)
        if not isinstance(how, JoinType):
            how = JoinType(how)
        joined_items = algo.sorted_join(
            iter_left=self.get_iter(),
            iter_right=right.get_iter(),
            key_function=key_function,
            merge_function=merge_function,
            order_function=bf.is_ordered(reverse=sorting_is_reversed, including=True),
            how=how,
        )
        stream = self.stream(list(joined_items) if self.is_in_memory() else joined_items, **self.get_static_meta())
        return self._assume_native(stream)

    def join(
            self,
            right: Native,
            key: UniKey,
            how: How = JoinType.Left,
            reverse: bool = False,
            is_sorted: bool = False,
            right_is_uniq: bool = False,
            allow_map_side: bool = True,
            force_map_side: bool = False,
            merge_function: Callable = fs.merge_two_items(),
            verbose: Optional[bool] = None,
    ) -> Native:
        on_map_side = force_map_side or (allow_map_side and right.can_be_in_memory())
        if on_map_side:
            stream = self.map_side_join(
                right, key=key, how=how,
                right_is_uniq=right_is_uniq,
                merge_function=merge_function,
            )
        else:
            if is_sorted:
                left = self
            else:
                left = self.sort(key, reverse=reverse, verbose=verbose)
                right = right.sort(key, reverse=reverse, verbose=verbose)
            stream = left.sorted_join(
                right, key=key, how=how,
                merge_function=merge_function,
                sorting_is_reversed=reverse,
            )
        return self._assume_native(stream)

    def split_to_disk_by_step(
            self,
            step: Count = None,
            sort_each_by: UniKey = None,
            reverse: bool = False,
            verbose: bool = True,
    ) -> list:
        if isinstance(self, RegularStreamInterface) or hasattr(self, 'get_item_type'):
            item_type = self.get_item_type()
        else:
            item_type = ItemType.Any
        result_parts = list()
        for part_no, sm_part in enumerate(self.to_iter().split_to_iter_by_step(step)):
            is_last_part = sm_part.get_count() < step
            is_single_part = is_last_part and part_no == 0
            file_part = self.get_tmp_files().file(
                part_no,
                content_format=ContentType.JsonFile,
                encoding=self.get_encoding(),
            )
            part_fn = file_part.get_path()
            if is_single_part:
                self.log('Sorting single part without saving...', verbose=verbose)
            else:
                self.log(f'Sorting part {part_no} and saving into {part_fn} ... ', verbose=verbose)
            if sort_each_by:
                sm_part = sm_part.memory_sort(
                    key=sort_each_by,
                    reverse=reverse,
                    verbose=verbose,
                )
            if not is_single_part:
                self.log('Writing {part_fn} ...', end='\r', verbose=verbose)
                sm_part = sm_part.to_json().write_to(
                    file_part,
                ).map_to_type(
                    fs.json_loads(),
                    stream_type=item_type,
                )
            result_parts.append(sm_part)
        return result_parts

    @staticmethod
    def _assume_native(stream) -> Native:
        return stream

    def is_empty(self) -> Optional[bool]:
        count = self.get_count()
        if count:
            return False
        elif count == 0:
            return True
        else:
            return None

    def update_count(self, force: bool = False, skip_iter: bool = True) -> Native:
        if force and not self.is_in_memory():
            self.collect()
        if self.is_in_memory():
            count = len(self.get_list())
            self.set_expected_count(count)
            self.set_estimated_count(count)
        elif not skip_iter:
            message = 'use update_count(force=True) for count in memory or final_count() for count spending iterator'
            raise ValueError('Cannot count items in iterator ({})'.format(message))
        return self

    def get_count(self, in_memory: Optional[bool] = None, final: bool = False) -> Count:
        if not Auto.is_defined(in_memory):
            in_memory = self.is_in_memory()
        if in_memory:
            data = self.get_list()
            self._count = len(data)
            self._data = data
            return self._count
        else:
            if final:
                return self.final_count()
            else:
                return self.get_expected_count()

    def get_str_count(self, default: str = '(unknown count)') -> str:
        if self.is_in_memory():
            return 'in memory {}'.format(self.get_count())
        else:
            return super().get_str_count(default=default)  # IterableStream.get_str_count()

    def get_str_description(self) -> str:
        return '{} items with meta {}'.format(self.get_str_count(), self.get_meta())

    @deprecated_with_alternative('get_shape_repr()')
    def get_description(self) -> str:
        return self.get_str_description()

    def get_tmp_files(self) -> TemporaryFilesMaskInterface:
        return self._tmp_files

    def remove_tmp_files(self) -> int:
        return self.get_tmp_files().remove_all()

    def get_encoding(self, default: str = 'utf8') -> str:
        tmp_files = self.get_tmp_files()
        if hasattr(tmp_files, 'get_encoding'):
            return tmp_files.get_encoding()
        else:
            return default

    def get_mask(self) -> str:
        tmp_files = self.get_tmp_files()
        if isinstance(tmp_files, TemporaryFilesMaskInterface) or hasattr(tmp_files, 'get_mask'):
            return tmp_files.get_mask()
        else:
            raise TypeError('Expected TemporaryFilesMaskInterface, got {}'.format(tmp_files))
