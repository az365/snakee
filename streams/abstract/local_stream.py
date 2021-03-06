from abc import ABC, abstractmethod
from typing import Optional, Union, Iterable, Callable
import json

try:  # Assume we're a sub-module in a package.
    from utils import (
        algo,
        arguments as arg,
        mappers as ms,
    )
    from streams.stream_type import StreamType
    from streams.abstract.iterable_stream import IterableStream, IterableStreamInterface
    from streams import stream_classes as sm
    from connectors.filesystem.temporary_interface import TemporaryFilesMaskInterface
    from functions import all_functions as fs
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import (
        algo,
        arguments as arg,
        mappers as ms,
    )
    from ..stream_type import StreamType
    from .iterable_stream import IterableStream, IterableStreamInterface
    from .. import stream_classes as sm
    from ...connectors.filesystem.temporary_interface import TemporaryFilesMaskInterface
    from ...functions import all_functions as fs

OptionalFields = Optional[Union[Iterable, str]]
DefaultStr = Union[str, arg.DefaultArgument]
Array = Union[list, tuple]
Key = Union[str, Array, Callable]
Step = Union[int, arg.DefaultArgument]
Verbose = Union[bool, arg.DefaultArgument]
OptStreamType = Union[StreamType, arg.DefaultArgument]
Stream = sm.StreamInterface
NativeInterface = IterableStreamInterface


class LocalStreamInterface(IterableStreamInterface, ABC):
    @abstractmethod
    def get_list(self) -> list:
        pass

    @abstractmethod
    def to_iter(self) -> NativeInterface:
        pass

    @abstractmethod
    def can_be_in_memory(self, step: Step = arg.DEFAULT) -> bool:
        pass

    @abstractmethod
    def to_memory(self) -> NativeInterface:
        pass

    @abstractmethod
    def collect(self) -> NativeInterface:
        pass

    def memory_sort(self, key: Key = fs.same(), reverse: bool = False, verbose: bool = False) -> NativeInterface:
        pass

    @abstractmethod
    def disk_sort(
            self,
            key: Key = fs.same(),
            reverse: bool = False,
            step: Step = arg.DEFAULT,
            verbose: bool = False,
    ) -> IterableStream:
        pass

    @abstractmethod
    def sort(self, *keys, reverse: bool = False, step: Step = arg.DEFAULT, verbose: bool = True) -> NativeInterface:
        pass

    @abstractmethod
    def sorted_join(
            self,
            right: NativeInterface,
            key: Key,
            how: str = 'left',
            sorting_is_reversed: bool = False,
    ) -> NativeInterface:
        pass

    @abstractmethod
    def join(
            self,
            right: NativeInterface,
            key: Key,
            how: str = 'left',
            reverse: bool = False,
            verbose: Verbose = arg.DEFAULT,
    ) -> NativeInterface:
        pass

    @abstractmethod
    def split_to_disk_by_step(
            self,
            step: Step = arg.DEFAULT,
            sort_each_by: Optional[str] = None,
            reverse: bool = False,
            verbose: bool = True,
    ) -> Iterable:
        pass


Native = LocalStreamInterface


class LocalStream(IterableStream, LocalStreamInterface):
    def __init__(
            self,
            data: Iterable,
            name: DefaultStr = arg.DEFAULT, check: bool = False,
            count: Optional[int] = None, less_than: Optional[int] = None,
            source=None, context=None,
            max_items_in_memory=arg.DEFAULT,
            tmp_files=arg.DEFAULT,
    ):
        count = arg.get_optional_len(data, count)
        less_than = less_than or count
        super().__init__(
            data=data, name=name, check=check,
            source=source, context=context,
            count=count, less_than=less_than,
            max_items_in_memory=max_items_in_memory,
        )
        self._tmp_files = arg.delayed_undefault(tmp_files, sm.get_tmp_mask, self.get_name())

    def get_list(self) -> list:
        return list(self.get_items())

    def is_file(self):
        if hasattr(self, 'is_leaf'):
            return self.is_leaf()
        else:
            return False

    def apply_to_data(self, function: Callable, dynamic: bool = False, *args, **kwargs):
        return self.stream(  # can be file
            self.get_calc(function, *args, **kwargs),
            ex=self._get_dynamic_meta_fields() if dynamic else None,
        )

    def is_in_memory(self) -> bool:
        if self.is_file():
            return False
        else:
            return arg.is_in_memory(self.get_data())

    def close(
            self,
            recursively: bool = False, return_closed_links: bool = False, remove_tmp_files: bool = True,
    ) -> Union[int, tuple]:
        result = super().close(recursively=recursively, return_closed_links=return_closed_links)
        if remove_tmp_files:
            self.remove_tmp_files()
        return result

    def to_iter(self) -> Native:
        return self.stream(
            self.get_iter(),
        )

    def can_be_in_memory(self, step: Step = arg.DEFAULT) -> bool:
        step = arg.undefault(step, self.max_items_in_memory)
        if self.is_in_memory() or step is None:
            return True
        else:
            return self.get_estimated_count() is not None and self.get_estimated_count() <= step

    def to_memory(self) -> Native:
        items_as_list_in_memory = self.get_list()
        return self.stream(
            items_as_list_in_memory,
            count=len(items_as_list_in_memory),
            check=False,
        )

    def collect(self) -> Native:
        return self.to_memory()

    def tail(self, count: int = 10) -> Native:
        total_count = self.get_count()
        if count:
            stream = self.skip(total_count - count)
        else:
            stream = super().tail(count)
        return self._assume_native(stream)

    def copy(self) -> Native:
        if self.is_in_memory():
            return self.stream(
                self.get_list().copy(),
            )
        else:  # is iterable generator
            return self._assume_native(super().copy())

    def get_tee_items(self, mem_copy: bool = False) -> Iterable:
        if self.is_in_memory():
            return self.copy().get_items() if mem_copy else self.get_items()
        else:
            return super().get_tee_items()

    def map_to(self, function: Callable, stream_type: OptStreamType = arg.DEFAULT) -> Stream:
        stream_type = arg.undefault(stream_type, self.get_stream_type, delayed=True)
        stream = self.stream(
            map(function, self.get_iter()),
            stream_type=stream_type,
        )
        if self.is_in_memory():
            stream = stream.to_memory()
        return self._assume_native(stream)

    def map(self, function: Callable, to: OptStreamType = arg.DEFAULT) -> Native:
        if arg.is_defined(to):
            self.get_logger().warning('to-argument for map() is deprecated, use map_to() instead')
            stream = self.map_to(function, stream_type=to)
        else:
            stream = super().map(function)
        if self.is_in_memory() and hasattr(stream, 'to_memory'):
            stream = stream.to_memory()
        return self._assume_native(stream)

    def filter(self, function: Callable) -> Native:
        filtered_items = self.get_filtered_items(function)
        if self.is_in_memory():
            filtered_items = list(filtered_items)
            count = len(filtered_items)
            return self.stream(
                filtered_items,
                count=count,
                less_than=count,
            )
        else:
            stream = super().filter(function)
            return self._assume_native(stream)

    def split(self, by: Union[int, list, tuple, Callable], count: Optional[int] = None) -> Iterable:
        for stream in super().split(by=by, count=count):
            if self.is_in_memory():
                stream = stream.to_memory()
            yield stream

    def memory_sort(self, key: Key = fs.same(), reverse: bool = False, verbose: Verbose = False) -> Native:
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
        self._count = len(sorted_items)
        return self.stream(sorted_items)

    def disk_sort(
            self,
            key: Key = fs.same(),
            reverse: bool = False,
            step: Step = arg.DEFAULT,
            verbose: Verbose = False,
    ) -> Native:
        step = arg.undefault(step, self.max_items_in_memory)
        key_function = fs.composite_key(key)
        stream_parts = self.split_to_disk_by_step(
            step=step,
            sort_each_by=key_function, reverse=reverse,
            verbose=verbose,
        )
        assert stream_parts, 'streams must be non-empty'
        iterables = [f.get_iter() for f in stream_parts]
        counts = [f.get_count() or 0 for f in stream_parts]
        self.log('Merging {} parts... '.format(len(iterables)), verbose=verbose)
        return self.stream(
            algo.merge_iter(
                iterables,
                key_function=key_function,
                reverse=reverse,
                post_action=self.get_tmp_files().remove_all,
            ),
            count=sum(counts),
        )

    def sort(self, *keys, reverse: bool = False, step: Step = arg.DEFAULT, verbose: Verbose = True) -> Native:
        keys = arg.update(keys)
        step = arg.undefault(step, self.max_items_in_memory)
        if len(keys) == 0:
            key_function = fs.same()
        else:
            key_function = fs.composite_key(keys)
        if self.can_be_in_memory():
            stream = self.memory_sort(key_function, reverse=reverse, verbose=verbose)
        else:
            stream = self.disk_sort(key_function, reverse=reverse, step=step, verbose=verbose)
        return self._assume_native(stream)

    def sorted_join(
            self,
            right: Native,
            key: Key,
            how: str = 'left',
            sorting_is_reversed: bool = False,
    ) -> Native:
        assert sm.is_stream(right)
        assert how in algo.JOIN_TYPES, 'only {} join types are supported ({} given)'.format(algo.JOIN_TYPES, how)
        keys = arg.update([key])
        joined_items = algo.sorted_join(
            iter_left=self.get_iter(),
            iter_right=right.get_iter(),
            key_function=fs.composite_key(keys),
            merge_function=ms.merge_two_items,
            order_function=fs.is_ordered(reverse=sorting_is_reversed, including=True),
            how=how,
        )
        return self.stream(
            list(joined_items) if self.is_in_memory() else joined_items,
            **self.get_static_meta()
        )

    def join(
            self,
            right: Native,
            key: Key,
            how='left',
            reverse: bool = False,
            verbose: Verbose = arg.DEFAULT,
    ) -> Native:
        stream = self.sort(
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
        return self._assume_native(stream)

    def split_to_disk_by_step(
            self,
            step: Step = arg.DEFAULT,
            sort_each_by: Key = None,
            reverse: bool = False,
            verbose: bool = True,
    ) -> list:
        result_parts = list()
        for part_no, sm_part in enumerate(self.to_iter().split_to_iter_by_step(step)):
            file_part = self.get_tmp_files().file(
                part_no,
                filetype='JsonFile',
                encoding=self.get_encoding(),
            )
            part_fn = file_part.get_path()
            self.log('Sorting part {} and saving into {} ... '.format(part_no, part_fn), verbose=verbose)
            if sort_each_by:
                sm_part = sm_part.memory_sort(
                    key=sort_each_by,
                    reverse=reverse,
                    verbose=verbose,
                )
            self.log('Writing {} ...'.format(part_fn), end='\r', verbose=verbose)
            sm_part = sm_part.to_json().write_to(
                file_part,
            ).map_to_type(
                json.loads,
                stream_type=sm.StreamType.AnyStream,
            )
            result_parts.append(sm_part)
        return result_parts

    def stream(self, data: Iterable, ex: OptionalFields = None, **kwargs) -> Native:
        stream = super().stream(data=data, ex=ex, **kwargs)
        return self._assume_native(stream)

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

    def get_count(
            self,
            in_memory: Union[bool, arg.DefaultArgument] = arg.DEFAULT,
            final: bool = False,
    ) -> Optional[int]:
        in_memory = arg.undefault(in_memory, self.is_in_memory())
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

    def get_str_count(self) -> str:
        if self.is_in_memory():
            return 'in memory {}'.format(self.get_count())
        else:
            return super().get_str_count()  # IterableStream.get_str_count()

    def get_description(self) -> str:
        return '{} items with meta {}'.format(self.get_str_count(), self.get_meta())

    def get_demo_example(self, count: int = 3) -> object:
        if self.is_in_memory():
            for i in self.get_items()[:count]:
                yield i
        else:
            yield from super().get_demo_example(count=count)

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
        return self.get_tmp_files().get_mask()
