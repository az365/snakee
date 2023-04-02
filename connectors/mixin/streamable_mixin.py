from abc import ABC
from typing import Optional, Iterable, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        IterableStreamInterface, StructInterface, Context, LeafConnectorInterface, StructMixinInterface,
        RegularStreamInterface, RowStream, StructStream, RecordStream, LineStream,
        ItemType, StreamType, OptionalFields, Array, Count,
    )
    from base.functions.arguments import get_generated_name
    from utils.decorators import deprecated_with_alternative
    from streams.mixin.columnar_mixin import ColumnarMixin
    from streams.stream_builder import StreamBuilder
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        IterableStreamInterface, StructInterface, Context, LeafConnectorInterface, StructMixinInterface,
        RegularStreamInterface, RowStream, StructStream, RecordStream, LineStream,
        ItemType, StreamType, OptionalFields, Array, Count,
    )
    from ...base.functions.arguments import get_generated_name
    from ...utils.decorators import deprecated_with_alternative
    from ...streams.mixin.columnar_mixin import ColumnarMixin
    from ...streams.stream_builder import StreamBuilder

Stream = Union[IterableStreamInterface, RegularStreamInterface]
Message = Union[str, Array, None]
Native = Union[Stream, LeafConnectorInterface]


class StreamableMixin(ColumnarMixin, ABC):
    @staticmethod
    def get_default_item_type() -> ItemType:
        """Returns ItemType expected while parsing this file/content."""
        return ItemType.Any

    @classmethod
    def get_stream_type(cls) -> StreamType:
        return StreamType.AnyStream

    def _get_stream_type(self, stream_type: ItemType = ItemType.Auto) -> StreamType:
        if stream_type in (ItemType.Auto, None):
            if hasattr(self, 'get_stream_type'):
                stream_type = self.get_stream_type()
            elif hasattr(self, 'get_default_stream_type'):
                stream_type = self.get_default_stream_type()
            else:
                item_type = self.get_default_item_type()
                stream_type = StreamType.detect(item_type)
        return stream_type

    def _get_item_type(self, stream_or_type: Union[ItemType, RegularStreamInterface] = ItemType.Auto) -> ItemType:
        if isinstance(stream_or_type, ItemType) or hasattr(stream_or_type, 'get_value_from_item'):
            if stream_or_type == ItemType.Auto:
                return self.get_default_item_type()
            else:
                return stream_or_type
        elif isinstance(stream_or_type, StreamType) or hasattr(stream_or_type, 'get_item_type'):
            return stream_or_type.get_item_type()
        elif hasattr(stream_or_type, 'get_default_item_type'):
            return stream_or_type.get_default_item_type()
        elif stream_or_type is None:
            return self.get_default_item_type()
        else:
            return ItemType.Any

    def _get_generated_stream_name(self) -> str:
        return get_generated_name('{}:stream'.format(self.get_name()), include_random=True, include_datetime=False)

    def _get_fast_count(self) -> Optional[int]:
        if isinstance(self, LeafConnectorInterface):  # or 'allow_slow_mode' in self.get_count.__annotations__:
            return self.get_count(allow_slow_mode=False)
        if hasattr(self.get_count, '__annotations__'):
            if isinstance(self, LeafConnectorInterface) or 'allow_slow_mode' in self.get_count.__annotations__:
                return self.get_count(allow_slow_mode=False)
        else:
            return self.get_count()

    def get_estimated_count(self) -> Optional[int]:
        count = None
        if hasattr(self, 'get_expected_count'):
            count = self.get_expected_count()
        if hasattr(self, 'get_less_than') and not count:
            count = self.get_less_than()
        if not count:
            count = self._get_fast_count()
        return count

    def _get_items_of_type(
            self,
            item_type: Optional[ItemType],
            verbose: Optional[bool] = None,
            step: Count = None,
            message: Optional[str] = None,
    ) -> Iterable:
        if hasattr(self, 'get_items_of_type'):
            return self.get_items_of_type(item_type, verbose=verbose, step=step, message=message)
        else:
            raise AttributeError('for get items object must be Connector and have get_items_of_type() method')

    def get_stream_kwargs(
            self,
            data: Optional[Iterable] = None,
            name: Optional[str] = None,
            verbose: Optional[bool] = None,
            step: Count = None,
            message: Optional[str] = None,
            **kwargs
    ) -> dict:
        """Returns kwargs for stream builder call.

        :returns: dict with kwargs for provide in stream builder arguments, i.e. *Stream(**self.get_stream_kwargs(data))
        """
        if not name:
            name = self._get_generated_stream_name()
        if data is None:
            item_type = self._get_item_type()
            data = self._get_items_of_type(item_type, verbose=verbose, step=step, message=message)
        result = dict(
            data=data, name=name, source=self,
            count=self._get_fast_count(), context=self.get_context(),
        )
        result.update(kwargs)
        return result

    def stream(
            self, data: Optional[Iterable] = None,
            stream_type: ItemType = ItemType.Auto,
            ex: OptionalFields = None,
            **kwargs
    ) -> Stream:
        return self.to_stream(data, stream_type=stream_type, ex=ex, **kwargs)

    def to_stream(
            self,
            data: Optional[Iterable] = None,
            name: Optional[str] = None,
            stream_type: ItemType = ItemType.Auto,
            ex: OptionalFields = None,
            step: Count = None,
            **kwargs
    ) -> Stream:
        if not name:
            name = self._get_generated_stream_name()
        if isinstance(stream_type, StreamType) or hasattr(stream_type, 'get_item_type'):
            item_type = stream_type.get_item_type()
        else:
            item_type = stream_type
        if data:
            struct_source = data
        else:
            data = self._get_items_of_type(item_type, verbose=kwargs.get('verbose', None), step=step)
            struct_source = self
        meta = self.get_compatible_meta(StreamBuilder.empty(), name=name, ex=ex, **kwargs)
        if 'count' not in meta and 'count' not in kwargs:
            meta['count'] = self._get_fast_count()
        if 'source' not in meta:
            meta['source'] = self
        stream = StreamBuilder.stream(data, stream_type=stream_type, **meta)
        if isinstance(struct_source, StructMixinInterface) or hasattr(struct_source, 'get_struct'):
            if isinstance(stream, StructMixinInterface) or hasattr(stream, 'set_struct'):
                stream.set_struct(struct_source.get_struct(), inplace=True)
        return self._assume_stream(stream)

    def to_stream_type(
            self,
            stream_type: ItemType,
            step: Count = None,
            verbose: Optional[bool] = None,
            message: Optional[str] = None,
            **kwargs,
    ) -> Stream:
        if stream_type in (ItemType.Auto, None):
            stream_type = self._get_stream_type()
        item_type = self._get_item_type(stream_type)
        if 'item_type' not in kwargs:
            kwargs['item_type'] = item_type
        if 'struct' not in kwargs:
            if isinstance(self, LeafConnectorInterface) or hasattr(self, 'get_struct'):
                struct = self.get_struct()
                if struct:
                    kwargs['struct'] = struct
        data = kwargs.pop('data', None)
        if not data:
            data = self._get_items_of_type(item_type, step=step, verbose=verbose, message=message)
        stream_kwargs = self.get_stream_kwargs(data=data, step=step, verbose=verbose, **kwargs)
        stream = StreamBuilder.stream(**stream_kwargs)
        return self._assume_stream(stream)

    def to_any_stream(self, step: Count = None, verbose: Optional[bool] = None, **kwargs) -> Stream:
        return self.to_stream_type(ItemType.Any, step=step, verbose=verbose, **kwargs)

    def to_line_stream(self, step: Count = None, verbose: Optional[bool] = None, **kwargs) -> LineStream:
        return self.to_stream_type(ItemType.Line, step=step, verbose=verbose, **kwargs)

    def to_record_stream(self, step: Count = None, verbose: Optional[bool] = None, **kwargs) -> RecordStream:
        return self.to_stream_type(ItemType.Record, step=step, verbose=verbose, **kwargs)

    def to_row_stream(self, step: Count = None, verbose: Optional[bool] = None, **kwargs) -> RowStream:
        return self.to_stream_type(ItemType.Row, step=step, verbose=verbose, **kwargs)

    @deprecated_with_alternative('to_stream()')
    def to_struct_stream(
            self,
            struct: Optional[StructInterface] = None,
            step: Count = None,
            verbose: Optional[bool] = None,
            **kwargs,
    ) -> StructStream:
        assert self._is_existing(), 'for get stream file must exists'
        if struct is None:
            if isinstance(self, StructMixinInterface) or hasattr(self, 'get_struct'):
                struct = self.get_struct()
            else:
                raise TypeError('for getting struct stream connector must have a struct property')
        kwargs['struct'] = struct
        return self.to_stream_type(StreamType.StructStream, step=step, verbose=verbose, **kwargs)

    def from_stream(self, stream: Stream, verbose: Optional[bool] = None) -> Native:
        if hasattr(self, 'write_stream'):
            return self.write_stream(stream, verbose=verbose)
        else:
            raise AttributeError

    def add_stream(self, stream: Stream, **kwargs) -> Stream:
        stream = self.to_stream(**kwargs).add_stream(stream)
        return self._assume_stream(stream)

    def take(self, count: Union[int, bool] = 1, inplace: bool = False) -> Stream:
        assert not inplace, 'for LeafConnector inplace-mode is not supported'
        stream = self.to_stream().take(count)
        return self._assume_stream(stream)

    def collect(self, skip_missing: bool = False, **kwargs) -> Stream:
        if self._is_existing():
            stream = self.to_stream(**kwargs)
            if hasattr(stream, 'collect'):
                stream = stream.collect()
            elif not skip_missing:
                raise TypeError('stream {} of type {} can not be collected'.format(stream, stream.get_stream_type()))
        elif skip_missing:
            stream = StreamBuilder.empty()
        else:
            raise FileNotFoundError('File {} not found'.format(self.get_name()))
        return self._assume_stream(stream)

    @staticmethod
    def _assume_stream(stream) -> Stream:
        return stream

    def _is_existing(self) -> Optional[bool]:
        if hasattr(self, 'is_existing'):
            return self.is_existing()
        else:
            raise AttributeError
