from abc import ABC
from typing import Optional, Callable, Iterable, Iterator, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        Stream, RegularStream, LineStream, RowStream, RecordStream, KeyValueStream, StructStream,
        StreamType, ItemType, Array, Columns, OptionalFields, Auto, AUTO,
    )
    from base.functions.arguments import get_names, get_list, update
    from content.items.simple_items import MutableRecord, MutableRow, ImmutableRow, SimpleRow
    from functions.secondary import all_secondary_functions as fs
    from utils.external import pd, DataFrame
    from streams.interfaces.regular_stream_interface import RegularStreamInterface
    from streams.abstract.iterable_stream import IterableStream
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        Stream, RegularStream, LineStream, RowStream, RecordStream, KeyValueStream, StructStream,
        StreamType, ItemType, Array, Columns, OptionalFields, Auto, AUTO,
    )
    from ...base.functions.arguments import get_names, get_list, update
    from ...content.items.simple_items import MutableRecord, MutableRow, ImmutableRow, SimpleRow
    from ...functions.secondary import all_secondary_functions as fs
    from ...utils.external import pd, DataFrame
    from ..interfaces.regular_stream_interface import RegularStreamInterface
    from ..abstract.iterable_stream import IterableStream

Native = RegularStream
AnyStream = Stream
AutoStreamType = Union[Auto, StreamType]  # deprecated, use AutoItemType instead
StreamItemType = Union[StreamType, ItemType, Auto]
OptionalArguments = Optional[Union[str, Iterable]]


class ConvertMixin(IterableStream, ABC):
    def get_rows(self, columns: Columns = AUTO) -> Iterator[SimpleRow]:
        if isinstance(self, RegularStreamInterface) or hasattr(self, 'get_item_type'):
            item_type = self.get_item_type()
        else:
            item_type = AUTO
        if columns == AUTO:
            if item_type == ItemType.Row:
                yield from self.get_items()
            elif item_type == ItemType.Record:
                if isinstance(self, RegularStreamInterface) or hasattr(self, 'get_columns'):
                    columns = self.get_columns()
                else:
                    raise TypeError(f'ConvertMixin.get_rows(): Expected RegularStream, got {self}')
                columns = get_names(columns)
                for r in self.get_items():
                    yield [r.get(c) for c in columns]
            elif item_type == ItemType.StructRow:
                for i in self.get_items():
                    yield i.get_data()
            elif item_type in (ItemType.Any, ItemType.Auto, AUTO):
                if StreamType.RecordStream.isinstance(self):
                    for r in self.get_items():
                        yield [r.get(c) for c in columns]
                else:
                    row_stream_class = StreamType.RowStream.get_class()
                    return row_stream_class._get_typing_validated_items(self.get_items())
            else:
                raise ValueError(f'ConvertMixin.get_rows(): item type {item_type} not supported.')
        else:
            return self._get_mapped_items(fs.composite_key(columns))

    def get_records(self, columns: Columns = AUTO) -> Iterable:
        item_type = self.get_item_type()
        struct = self.get_struct()
        assert isinstance(item_type, ItemType)
        if Auto.is_defined(columns):
            if item_type == ItemType.Record:
                return self.select(*columns).get_items()
            else:
                func = item_type.get_key_function(*columns, struct=self.get_struct())
                columns = get_names(columns)
                if len(columns) == 1:
                    return self._get_mapped_items(lambda i: {columns[0]: func(i)})
                else:
                    return self._get_mapped_items(lambda i: dict(zip(columns, func(i))))
        else:
            if item_type == ItemType.Record:
                return self.get_items()
            elif Auto.is_defined(struct):
                return self.get_records(struct.get_columns())
            else :
                return self._get_mapped_items(lambda i: dict(item=i))

    def get_dataframe(self, columns: Columns = None) -> DataFrame:
        if columns and hasattr(self, 'select'):
            data = self.select(*columns).get_data()
        else:
            data = self.get_data()
        if DataFrame:
            return DataFrame(data)

    def stream(
            self,
            data: Iterable,
            stream_type: StreamItemType = AUTO,
            ex: OptionalArguments = None,
            save_name: bool = True,
            save_count: bool = True,
            **kwargs
    ) -> Stream:
        if Auto.is_defined(stream_type):
            if isinstance(stream_type, str):
                try:
                    stream_type = StreamType(stream_type)
                except ValueError:  # stream_type is not a valid StreamType
                    stream_type = ItemType(stream_type)
            if isinstance(stream_type, ItemType):
                item_type_name = stream_type.get_name()
                stream_type_name = f'{item_type_name}Stream'
                stream_type = StreamType(stream_type_name)
            if isinstance(stream_type, StreamType) or hasattr(stream_type, 'get_stream_class'):
                stream_class = stream_type.get_stream_class()
            else:
                stream_class = stream_type.get_class()
            meta = self.get_compatible_meta(stream_class, ex=ex)
        else:
            stream_class = self.__class__
            meta = self.get_meta()
        if not save_name:
            meta.pop('name')
        if not save_count:
            meta.pop('count')
        meta.update(kwargs)
        if 'context' not in meta and hasattr(self, 'get_context'):
            meta['context'] = self.get_context()
        stream = stream_class(data, **meta)
        return stream

    def map_to_type(self, function: Callable, stream_type: AutoStreamType = AUTO) -> Stream:
        stream_type = Auto.delayed_acquire(stream_type, self.get_stream_type)
        result = self.stream(
            map(function, self.get_items()),
            stream_type=stream_type,
        )
        if hasattr(self, 'is_in_memory'):
            if self.is_in_memory():
                return result.to_memory()
        return result

    # @deprecated_with_alternative('map_to_type()')
    def map_to_records(self, function: Callable) -> RecordStream:
        stream = self.map_to_type(
            function,
            stream_type=StreamType.RecordStream,
        )
        return self._assume_native(stream)

    # @deprecated_with_alternative('map_to_type()')
    def map_to_any(self, function: Callable) -> AnyStream:
        return self.map_to_type(
            function,
            stream_type=StreamType.AnyStream,
        )

    def to_any_stream(self) -> AnyStream:
        return self.stream(
            self.get_items(),
            stream_type=StreamType.AnyStream,
        )

    def to_line_stream(
            self,
            delimiter: Union[str, Auto] = AUTO,
            columns: Columns = AUTO,
            add_title_row: Union[bool, Auto] = AUTO,
    ) -> LineStream:
        stream_type = self.get_stream_type()
        delimiter = Auto.acquire(delimiter, '\t' if stream_type == StreamType.RowStream else None)
        stream = self
        if stream.get_stream_type() == StreamType.RecordStream:
            assert isinstance(stream, RegularStream) or hasattr(stream, 'get_columns'), 'got {}'.format(stream)
            columns = Auto.acquire(columns, stream.get_columns, delayed=True)
            add_title_row = Auto.acquire(add_title_row, True)
            stream = stream.to_row_stream(columns=columns, add_title_row=add_title_row)
        if delimiter:
            func = delimiter.join
        else:
            func = str
        stream = self.stream(
            stream._get_mapped_items(func),
            stream_type=StreamType.LineStream,
        )
        return self._assume_native(stream)

    def to_json(self, *args, **kwargs) -> LineStream:
        items = self._get_mapped_items(fs.json_dumps(*args, **kwargs))
        stream = self.stream(items, stream_type=StreamType.LineStream)
        return self._assume_native(stream)

    def to_record_stream(self, *args, **kwargs) -> RecordStream:
        if 'function' in kwargs:
            func = kwargs.pop('function')
            items = self._get_mapped_items(lambda i: func(i, *args, **kwargs))
        elif 'columns' in kwargs and len(kwargs) == 1:
            columns = kwargs.pop('columns')
            assert not args
            items = self.get_records(columns)
        elif self.get_stream_type() == StreamType.RecordStream:
            return self
        elif kwargs:  # and not args
            assert not args
            return self.to_any_stream().select(**kwargs)
        elif args:  # and not kwargs
            if len(kwargs) == 1:
                if callable(args[0]):
                    items = self._get_mapped_items(args[0])
                else:
                    items = self.get_records(args[0])
            else:
                items = self.get_records(args)
        else:  # not (args or kwargs):
            if hasattr(self, 'get_records'):
                items = self.get_records()
            else:
                items = self._get_mapped_items(lambda i: dict(item=i))
        stream = self.stream(
            items,
            stream_type=StreamType.RecordStream,
        )
        return self._assume_native(stream)

    def to_row_stream(self, *args, **kwargs) -> RowStream:
        function, delimiter = None, None
        if 'function' in kwargs:
            function = kwargs.pop('function')
        elif args:
            if callable(args[0]):
                function, args = args[0], args[1:]
            elif self.get_stream_type() in (StreamType.LineStream, StreamType.AnyStream):
                delimiter, args = args[0], args[1:]
        elif self.get_stream_type() == StreamType.RecordStream:
            add_title_row = kwargs.pop('add_title_row', None)
            columns = update(args, kwargs.pop('columns', None))
            assert isinstance(self, RecordStream)
            if not columns:
                columns = self.get_columns()
            function = self.get_rows(columns=columns, add_title_row=add_title_row)
        elif 'delimiter' in kwargs and self.get_stream_type() in (StreamType.LineStream, StreamType.AnyStream):
            delimiter = kwargs.pop('delimiter')
        elif args:
            assert not kwargs
            return self.to_any_stream().select(*args)
        if function:
            items = self._get_mapped_items(lambda i: function(i, *args, **kwargs))
        elif delimiter:
            csv_reader = fs.csv_reader(delimiter=delimiter, *args, **kwargs)
            items = csv_reader(self.get_items())
        else:
            items = self.get_items()
        stream = self.stream(items, stream_type=StreamType.RowStream)
        return self._assume_native(stream)

    def to_key_value_stream(self, key: Callable = fs.first(), value: Callable = fs.second()) -> KeyValueStream:
        if isinstance(key, (list, tuple)):
            key = fs.composite_key(key)
        if isinstance(value, (list, tuple)):
            value = fs.composite_key(value)
        stream = self.stream(
            self._get_mapped_items(lambda i: (key(i), value(i))),
            stream_type=StreamType.KeyValueStream,
        )
        return self._assume_native(stream)

    # @deprecated_with_alternative('ConvertMixin.to_key_value_stream()')
    def to_pairs(self, *args, **kwargs) -> KeyValueStream:
        if StreamType.LineStream:
            return self.to_row_stream(*args, **kwargs).to_key_value_stream()
        else:
            return self.to_key_value_stream(*args, **kwargs)

    # @deprecated
    def to_stream(self, stream_type: AutoStreamType = AUTO, *args, **kwargs) -> Stream:
        stream_type = Auto.acquire(stream_type, self.get_stream_type())
        method_suffix = StreamType.of(stream_type).get_method_suffix()
        method_name = 'to_{}'.format(method_suffix)
        stream_method = self.__getattribute__(method_name)
        return stream_method(stream_type, *args, **kwargs)

    @staticmethod
    def _assume_native(stream) -> Native:
        return stream
