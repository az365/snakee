from typing import Union, Iterable, Callable, Optional
from inspect import isclass

try:  # Assume we're a sub-module in a package.
    from utils import (
        arguments as arg,
        items as it,
        selection as sf,
    )
    from items.item_type import ItemType
    from streams.stream_type import StreamType
    from streams import stream_classes as sm
    from selection import selection_classes as sn
    from connectors.abstract.connector_interface import ConnectorInterface
    from base.interfaces.context_interface import ContextInterface
    from connectors.filesystem.temporary_interface import TemporaryFilesMaskInterface
    from utils.decorators import deprecated_with_alternative
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import (
        arguments as arg,
        items as it,
        selection as sf,
    )
    from ...items.item_type import ItemType
    from ..stream_type import StreamType
    from .. import stream_classes as sm
    from ...selection import selection_classes as sn
    from ...connectors.abstract.connector_interface import ConnectorInterface
    from ...base.interfaces.context_interface import ContextInterface
    from ...connectors.filesystem.temporary_interface import TemporaryFilesMaskInterface
    from ...utils.decorators import deprecated_with_alternative

OptionalFields = Optional[Union[str, Iterable]]
OptStreamType = Union[StreamType]
Stream = sm.StreamInterface
Native = sm.RegularStreamInterface
Data = Union[Iterable, arg.Auto]
Name = Union[str, arg.Auto]
Count = Union[int, arg.Auto]
Context = Optional[ContextInterface]
Source = Optional[ConnectorInterface]
TmpMask = Union[TemporaryFilesMaskInterface, arg.Auto]


class AnyStream(sm.LocalStream, sm.ConvertMixin, sm.RegularStreamInterface):
    def __init__(
            self,
            data,
            name: Name = arg.AUTO,
            count: Optional[int] = None,
            less_than: Optional[int] = None,
            source: Source = None,
            context: Context = None,
            max_items_in_memory: Count = arg.AUTO,
            tmp_files: TmpMask = arg.AUTO,
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
    def get_item_type() -> it.ItemType:
        return it.ItemType.Any

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

    def select(self, *columns, use_extended_method: bool = True, **expressions) -> Stream:
        if columns and not expressions:
            target_stream_type = sm.StreamType.RowStream
            target_item_type = it.ItemType.Row
            input_item_type = it.ItemType.Any
        elif expressions and not columns:
            target_stream_type = sm.StreamType.RecordStream
            target_item_type = it.ItemType.Record
            input_item_type = it.ItemType.Any
        else:
            target_stream_type = sm.StreamType.AnyStream
            target_item_type = it.ItemType.Auto
            input_item_type = it.ItemType.Auto
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

    def map_to_type(self, function: Callable, stream_type: OptStreamType = arg.AUTO) -> Native:
        stream = super().map_to(function=function, stream_type=stream_type)
        return self._assume_native(stream)

    def map(self, function: Callable, to: OptStreamType = arg.AUTO) -> Native:
        if arg.is_defined(to):
            self.get_logger().warning('to-argument for map() is deprecated, use map_to() instead')
            stream = super().map_to(function, stream_type=to)
        else:
            stream = super().map(function)
        return self._assume_native(stream)

    def flat_map(self, function: Callable, to: OptStreamType = arg.AUTO) -> Stream:
        def get_items():
            for i in self.get_iter():
                yield from function(i)
        to = arg.acquire(to, self.get_stream_type())
        stream_class = sm.StreamType(to).get_class()
        new_props_keys = stream_class([]).get_meta().keys()
        props = {k: v for k, v in self.get_meta().items() if k in new_props_keys}
        props.pop('count')
        return stream_class(
            get_items(),
            **props
        )

    @deprecated_with_alternative('map()')
    def native_map(self, function: Callable) -> Native:
        return self.stream(
            map(function, self.get_items()),
        )

    def apply_to_data(
            self,
            function: Callable,
            *args,
            dynamic: bool = True,
            stream_type: OptStreamType = arg.AUTO,
            **kwargs
    ) -> Stream:
        return self.stream(
            self.get_calc(function, *args, **kwargs),
            stream_type=stream_type,
            ex=self._get_dynamic_meta_fields() if dynamic else None,
        )

    def sorted_group_by(self, *keys, values: Optional[Iterable] = None, as_pairs: bool = False) -> Stream:
        raise NotImplemented

    def group_by(self, *keys, values: Optional[Iterable] = None, as_pairs: bool = False) -> Stream:
        raise NotImplemented

    @staticmethod
    def _assume_stream(stream) -> Stream:
        return stream

    @staticmethod
    def _assume_native(stream) -> Native:
        return stream

    def stream(self, data: Iterable, ex: OptionalFields = None, **kwargs) -> Native:
        stream = self.to_stream(data, ex=ex, **kwargs)
        return self._assume_native(stream)

    def to_stream(
            self,
            data: Data = arg.AUTO,
            stream_type: OptStreamType = arg.AUTO,
            ex: OptionalFields = None,
            **kwargs
    ) -> Stream:
        stream_type = arg.delayed_acquire(stream_type, self.get_stream_type)
        data = arg.delayed_acquire(data, self.get_data)
        if isinstance(stream_type, str):
            stream_class = StreamType(stream_type).get_class()
        elif isclass(stream_type):
            stream_class = stream_type
        else:
            stream_class = stream_type.get_class()
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
            cls,
            filename,
            encoding=None, gzip=False,
            skip_first_line=False, max_count=None,
            check=arg.AUTO,
            verbose=False,
    ) -> Stream:
        return sm.LineStream.from_text_file(
            filename,
            encoding=encoding, gzip=gzip,
            skip_first_line=skip_first_line, max_count=max_count,
            check=check,
            verbose=verbose,
        ).parse_json(
            to=cls.__name__,
        )
