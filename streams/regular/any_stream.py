from typing import Union, Iterable, Generator, Callable, Optional
from inspect import isclass

try:  # Assume we're a submodule in a package.
    from utils import arguments as arg, selection as sf
    from utils.decorators import deprecated_with_alternative
    from interfaces import (
        Stream, LocalStream, RegularStreamInterface, Context, Connector, TmpFiles,
        StreamType, ItemType,
        Name, Count, Columns, OptionalFields, Source, Array, ARRAY_TYPES,
        AUTO, Auto, AutoCount,
    )
    from functions.primary import items as it
    from content.selection import selection_classes as sn
    from streams.abstract.local_stream import LocalStream
    from streams.mixin.convert_mixin import ConvertMixin
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg, selection as sf
    from ...utils.decorators import deprecated_with_alternative
    from ...interfaces import (
        Stream, LocalStream, RegularStreamInterface, Context, Connector, TmpFiles,
        StreamType, ItemType,
        Name, Count, Columns, OptionalFields, Source, Array, ARRAY_TYPES,
        AUTO, Auto, AutoCount,
    )
    from ...functions.primary import items as it
    from ...content.selection import selection_classes as sn
    from ..abstract.local_stream import LocalStream
    from ..mixin.convert_mixin import ConvertMixin

Native = Union[LocalStream, RegularStreamInterface]
Data = Union[Auto, Iterable]
AutoStreamType = Union[Auto, StreamType]


class AnyStream(LocalStream, ConvertMixin, RegularStreamInterface):
    def __init__(
            self, data, name: Name = AUTO,
            count: Count = None, less_than: Count = None,
            source: Source = None, context: Context = None,
            max_items_in_memory: Count = AUTO, tmp_files: TmpFiles = AUTO,
            check: bool = False,
    ):
        super().__init__(
            data, name=name, check=check,
            count=count, less_than=less_than,
            source=source, context=context,
            max_items_in_memory=max_items_in_memory,
            tmp_files=tmp_files,
        )

    @staticmethod
    def get_item_type() -> ItemType:
        return ItemType.Any

    def get_columns(self) -> Optional[Iterable]:
        return None

    def get_detected_columns(self, count) -> list:
        return [self.get_item_type().get_value()]

    def filter(self, *functions) -> Native:
        def filter_function(item):
            for f in functions:
                if not f(item):
                    return False
            return True
        stream = super().filter(filter_function)
        return self._assume_native(stream)

    def select(self, *columns, use_extended_method: bool = True, **expressions) -> Native:
        if columns and not expressions:
            target_stream_type = StreamType.RowStream
            target_item_type = ItemType.Row
            input_item_type = ItemType.Any
        elif expressions and not columns:
            target_stream_type = StreamType.RecordStream
            target_item_type = ItemType.Record
            input_item_type = ItemType.Any
        else:
            target_stream_type = StreamType.AnyStream
            target_item_type = ItemType.Auto
            input_item_type = ItemType.Auto
        if use_extended_method:
            selection_method = sn.select
        else:
            selection_method = sf.select
        select_function = selection_method(
            *columns, **expressions,
            target_item_type=target_item_type, input_item_type=input_item_type,
            logger=self.get_logger(), selection_logger=self.get_selection_logger(),
        )
        return self.map_to_type(
            function=select_function,
            stream_type=target_stream_type,
        )

    def map_to_type(self, function: Callable, stream_type: AutoStreamType = AUTO) -> Native:
        stream = super().map_to(function=function, stream_type=stream_type)
        return self._assume_native(stream)

    def map(self, function: Callable, to: AutoStreamType = AUTO) -> Native:
        if arg.is_defined(to):
            self.get_logger().warning('to-argument for map() is deprecated, use map_to() instead')
            stream = super().map_to(function, stream_type=to)
        else:
            stream = super().map(function)
        return self._assume_native(stream)

    def flat_map(self, function: Callable, to: AutoStreamType = AUTO) -> Stream:
        if arg.is_defined(to):
            stream_class = StreamType.detect(to).get_class()
        else:
            stream_class = self.__class__
        new_props_keys = stream_class([]).get_meta().keys()
        props = {k: v for k, v in self.get_meta().items() if k in new_props_keys}
        props.pop('count')
        items = self._get_mapped_items(function=function, flat=True)
        return stream_class(items, **props)

    @deprecated_with_alternative('map()')
    def native_map(self, function: Callable) -> Native:
        items = map(function, self.get_items())
        stream = self.stream(items)
        return self._assume_native(stream)

    def apply_to_data(
            self,
            function: Callable,
            *args,
            dynamic: bool = True,
            stream_type: AutoStreamType = AUTO,
            **kwargs
    ) -> Stream:
        return self.stream(
            self._get_calc(function, *args, **kwargs),
            stream_type=stream_type,
            ex=self._get_dynamic_meta_fields() if dynamic else None,
        )

    @staticmethod
    def _get_tuple_function(*functions) -> Callable:
        return lambda r: tuple([f(r) for f in functions])

    def _get_key_function(self, functions: Array, take_hash: bool = False) -> Callable:
        if functions:
            msg = 'Expected Sequence[Callable], got {}'
            assert min([isinstance(f, Callable) for f in functions]), msg.format(functions)
        if len(functions) == 0:
            raise ValueError('key must be defined')
        elif len(functions) == 1:
            key_function = functions[0]
        else:
            key_function = self._get_tuple_function(functions)
        if take_hash:
            return lambda r: hash(key_function(r))
        else:
            return key_function

    def _get_groups(self, key_function: Callable, as_pairs: bool) -> Generator:
        accumulated = list()
        prev_k = None
        for r in self.get_items():
            k = key_function(r)
            if (k != prev_k) and accumulated:
                yield (prev_k, accumulated) if as_pairs else accumulated
                accumulated = list()
            prev_k = k
            accumulated.append(r)
        if as_pairs:
            yield prev_k, accumulated
        else:
            yield accumulated

    def sorted_group_by(
            self,
            *keys,
            values: Optional[Iterable] = None,
            as_pairs: bool = False,
            take_hash: bool = False,
    ) -> Stream:
        assert not values, 'For AnyStream.sorted_group_by() values option not supported'
        key_function = self._get_key_function(keys, take_hash=take_hash)
        iter_groups = self._get_groups(key_function, as_pairs=as_pairs)
        if as_pairs:
            stream_builder = StreamType.KeyValueStream.get_class()
            stream_groups = stream_builder(iter_groups, value_stream_type=self.get_stream_type())
        else:
            stream_builder = StreamType.RowStream.get_class()
            stream_groups = stream_builder(iter_groups, check=False)
        if self.is_in_memory():
            return stream_groups.to_memory()
        else:
            stream_groups.set_estimated_count(self.get_count() or self.get_estimated_count(), inplace=True)
            return stream_groups

    def group_by(
            self,
            *keys,
            values: Optional[Iterable] = None,
            as_pairs: bool = False,
            take_hash: bool = True,
            step: AutoCount = AUTO,
            verbose: bool = True,
    ) -> Stream:
        if as_pairs:
            key_for_sort = keys
        else:
            key_for_sort = self._get_key_function(keys, take_hash=take_hash)
        return self.sort(
            key_for_sort,
            step=step,
        ).sorted_group_by(
            *keys,
            values=values,
            as_pairs=as_pairs,
        )

    @staticmethod
    def _assume_stream(stream) -> Stream:
        return stream

    @staticmethod
    def _assume_native(stream) -> Native:
        return stream

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
        elif isinstance(stream_type, StreamType) or hasattr(stream_type, 'get_class'):
            stream_class = stream_type.get_class()
        else:
            raise TypeError('AnyStream.to_stream(data, stream_type): expected StreamType, got {}'.format(stream_type))
        if not arg.is_defined(data):
            if hasattr(self, 'get_items_of_type'):
                item_type = stream_class.get_item_type()
                data = self.get_items_of_type(item_type)
            else:
                data = self.get_data()
        meta = self.get_compatible_meta(stream_class, ex=ex)
        meta.update(kwargs)
        if 'count' not in meta:
            meta['count'] = self.get_count()
        if 'source' not in meta:
            meta['source'] = self.get_source()
        stream = stream_class(data, **meta)
        return self._assume_stream(stream)

    @classmethod
    @deprecated_with_alternative('connectors.filesystem.local_file.JsonFile.to_stream()')
    def from_json_file(
            cls, filename, encoding=None,
            skip_first_line=False, max_count=None,
            check=AUTO, verbose=False,
    ) -> Stream:
        line_stream_class = StreamType.LineStream.get_class()
        return line_stream_class.from_text_file(
            filename, encoding=encoding,
            skip_first_line=skip_first_line, max_count=max_count,
            check=check, verbose=verbose,
        ).parse_json(to=cls.__name__)
