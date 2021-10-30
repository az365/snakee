from abc import ABC, abstractmethod
from typing import Iterable, Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from interfaces import (
        IterableStreamInterface, StructInterface, Context,
        RegularStream, RowStream, StructStream, RecordStream, LineStream,
        ItemType, StreamType,
        Auto, AUTO, AutoBool, AutoCount, AutoName, Array, OptionalFields,
    )
    from streams.mixin.iterable_mixin import IterableStreamMixin
    from connectors.abstract.leaf_connector import LeafConnector
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...interfaces import (
        IterableStreamInterface, StructInterface, Context,
        RegularStream, RowStream, StructStream, RecordStream, LineStream,
        ItemType, StreamType,
        Auto, AUTO, AutoBool, AutoCount, AutoName, Array, OptionalFields,
    )
    from ...streams.mixin.iterable_mixin import IterableStreamMixin
    from ..abstract.leaf_connector import LeafConnector

Stream = Union[IterableStreamInterface, RegularStream]
Message = Union[AutoName, Array]
Native = Union[LeafConnector, Stream]


class StreamFileMixin(IterableStreamMixin, ABC):
    @staticmethod
    def get_default_item_type() -> ItemType:
        return ItemType.Any

    @classmethod
    def get_stream_type(cls):
        return StreamType.AnyStream

    def _get_stream_type(self, stream_type: Union[StreamType, Auto] = AUTO) -> StreamType:
        if not arg.is_defined(stream_type):
            if hasattr(self, 'get_stream_type'):
                stream_type = self.get_stream_type()
            elif hasattr(self, 'get_default_stream_type'):
                stream_type = self.get_default_item_type()
            else:
                stream_type = StreamType.LineStream
        return stream_type

    def _get_stream_class(self, stream_type: Union[StreamType, Auto] = AUTO):
        stream_type = self._get_stream_type(stream_type)
        return stream_type.get_class()

    def _get_item_type(self, stream: Union[StreamType, RegularStream, Auto] = AUTO) -> ItemType:
        if isinstance(stream, StreamType) or hasattr(stream, 'get_class'):
            stream_class = self._get_stream_class(stream)
        elif arg.is_defined(stream):
            stream_class = stream
        else:
            stream_class = self._get_stream_class()
        assert isinstance(stream_class, RegularStream) or hasattr(stream_class, 'get_item_type')
        return stream_class.get_item_type()

    def _get_generated_stream_name(self) -> str:
        return arg.get_generated_name('{}:stream'.format(self.get_name()), include_random=True, include_datetime=False)

    @abstractmethod
    def get_items(
            self,
            item_type: Union[ItemType, Auto] = AUTO,
            verbose: AutoBool = AUTO,
            step: AutoCount = AUTO,
    ) -> Iterable:
        pass

    def get_stream_kwargs(
            self,
            data: Union[Iterable, Auto] = AUTO,
            name: AutoName = AUTO,
            verbose: AutoBool = AUTO,
            step: AutoCount = AUTO,
            **kwargs
    ) -> dict:
        verbose = arg.acquire(verbose, self.is_verbose())
        data = arg.delayed_acquire(data, self.get_items, verbose=verbose, step=step)
        name = arg.delayed_acquire(name, self._get_generated_stream_name)
        result = dict(
            data=data, name=name, source=self,
            count=self.get_count(), context=self.get_context(),
        )
        result.update(kwargs)
        return result

    def stream(
            self, data: Union[Iterable, Auto] = AUTO,
            stream_type: Union[StreamType, Auto] = AUTO,
            ex: OptionalFields = None,
            **kwargs
    ) -> Stream:
        return self.to_stream(data, stream_type=stream_type, ex=ex, **kwargs)

    def to_stream(
            self,
            data: Union[Iterable, Auto] = AUTO,
            name: AutoName = AUTO,
            stream_type: Union[StreamType, Auto] = AUTO,
            ex: OptionalFields = None,
            step: AutoCount = AUTO,
            **kwargs
    ) -> Stream:
        name = arg.delayed_acquire(name, self._get_generated_stream_name)
        stream_type = self._get_stream_type(stream_type)
        stream_class = self._get_stream_class(stream_type)
        if hasattr(stream_class, 'get_item_type'):
            item_type = stream_class.get_item_type()
        else:
            item_type = AUTO
        if not arg.is_defined(data):
            data = self.get_items(item_type=item_type, verbose=kwargs.get('verbose', AUTO), step=step)
        meta = self.get_compatible_meta(stream_class, name=name, ex=ex, **kwargs)
        if 'count' not in meta:
            meta['count'] = self.get_count()
        if 'source' not in meta:
            meta['source'] = self
        stream = stream_class(data, **meta)
        return self._assume_stream(stream)

    def to_stream_type(
            self,
            stream_type: StreamType,
            step: AutoCount = AUTO,
            verbose: AutoBool = AUTO,
            **kwargs,
    ):
        item_type = self._get_item_type(stream_type)
        data = self.get_items(item_type=item_type, step=step, verbose=verbose)
        stream_kwargs = self.get_stream_kwargs(data=data, step=step, verbose=verbose, **kwargs)
        return stream_type.stream(**stream_kwargs)

    def to_any_stream(self, step: AutoCount = AUTO, verbose: AutoBool = AUTO, **kwargs) -> Stream:
        return self.to_stream_type(StreamType.AnyStream, step=step, verbose=verbose, **kwargs)

    def to_line_stream(self, step: AutoCount = AUTO, verbose: AutoBool = AUTO, **kwargs) -> LineStream:
        return self.to_stream_type(StreamType.LineStream, step=step, verbose=verbose, **kwargs)

    def to_record_stream(self, step: AutoCount = AUTO, verbose: AutoBool = AUTO, **kwargs) -> RowStream:
        return self.to_stream_type(StreamType.RecordStream, step=step, verbose=verbose, **kwargs)

    def to_row_stream(self, step: AutoCount = AUTO, verbose: AutoBool = AUTO, **kwargs) -> RowStream:
        return self.to_stream_type(StreamType.RowStream, step=step, verbose=verbose, **kwargs)

    def to_struct_stream(
            self,
            struct: Union[StructInterface, Auto] = AUTO,
            step: AutoCount = AUTO,
            verbose: AutoBool = AUTO,
            **kwargs,
    ) -> StructStream:
        if not arg.is_defined(struct):
            if hasattr(self, 'get_struct'):
                struct = self.get_struct()
            elif hasattr(self, 'get_content_format'):
                struct = self.get_content_format().get_struct()
        kwargs['struct'] = struct
        return self.to_stream_type(StreamType.StructStream, step=step, verbose=verbose, **kwargs)

    def from_stream(self, stream: Stream, verbose: AutoBool = AUTO) -> Native:
        return self.write_stream(stream, verbose=verbose)

    @abstractmethod
    def write_stream(
            self,
            stream: IterableStreamInterface,
            add_title_row: AutoBool = AUTO,
            verbose: AutoBool = AUTO,
    ) -> Native:
        pass

    @abstractmethod
    def write_lines(self, lines: Iterable, verbose: AutoBool = AUTO) -> Native:
        pass

    def add_stream(self, stream: Stream, **kwargs) -> Stream:
        stream = self.to_stream(**kwargs).add_stream(stream)
        return self._assume_stream(stream)

    def collect(self, skip_missing: bool = False, **kwargs) -> Stream:
        if self.is_existing():
            stream = self.to_stream(**kwargs)
            if hasattr(stream, 'collect'):
                stream = stream.collect()
            elif not skip_missing:
                raise TypeError('stream {} of type {} can not be collected'.format(stream, stream.get_stream_type()))
        elif skip_missing:
            stream = self._get_stream_class()([])
        else:
            raise FileNotFoundError('File {} not found'.format(self.get_name()))
        return self._assume_stream(stream)

    @staticmethod
    def _assume_stream(obj) -> Stream:
        return obj

    def get_children(self) -> dict:
        return self._data

    @abstractmethod
    def get_context(self) -> Context:
        pass

    @abstractmethod
    def is_verbose(self) -> bool:
        pass

    @abstractmethod
    def is_existing(self) -> bool:
        pass
