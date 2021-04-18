from abc import ABC
from typing import Optional, Union, Iterable, Any
import sys
import json
import csv
import pandas as pd

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from functions import item_functions as fs
    from streams.interfaces.regular_stream_interface import RegularStreamInterface
    from streams.abstract.iterable_stream import IterableStream
    from streams import stream_classes as sm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...functions import item_functions as fs
    from ..interfaces.regular_stream_interface import RegularStreamInterface
    from ..abstract.iterable_stream import IterableStream
    from .. import stream_classes as sm

OptionalArguments = Optional[Union[str, Iterable]]
Stream = Union[RegularStreamInterface, Any]
AnyStream = Stream
LineStream = Stream
RecordStream = Stream
RowStream = Stream
KeyValueStream = Stream
SchemaStream = Stream

max_int = sys.maxsize
while True:  # To prevent _csv.Error: field larger than field limit (131072)
    try:  # decrease the max_int value by factor 10 as long as the OverflowError occurs.
        csv.field_size_limit(max_int)
        break
    except OverflowError:
        max_int = int(max_int / 10)


class ConvertMixin(IterableStream, ABC):
    def get_rows(self, columns: Union[tuple, arg.DefaultArgument] = arg.DEFAULT) -> Iterable:
        if columns == arg.DEFAULT:
            if sm.StreamType.RecordStream.isinstance(self):
                for r in self.get_items():
                    yield [r.get(c) for c in columns]
            else:
                return sm.RowStream.get_typing_validated_items(self.get_items())
        else:
            return self.get_mapped_items(fs.composite_key(columns))

    def get_records(self, columns: Union[tuple, arg.DefaultArgument] = arg.DEFAULT) -> Iterable:
        if columns == arg.DEFAULT:
            return self.get_mapped_items(lambda i: dict(item=i))
        else:
            return self.get_mapped_items(lambda i: dict(zip(columns, fs.composite_key(columns)(i))))

    def get_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(self.get_data())

    def stream(
            self,
            data: Iterable,
            stream_type=arg.DEFAULT,  # tmp
            ex: OptionalArguments = None,
            save_name: bool = True, save_count: bool = True, **kwargs
    ) -> Stream:
        stream_type = arg.undefault(stream_type, self.get_stream_type())
        if isinstance(stream_type, str):
            stream_class = sm.StreamType(stream_type).get_class()
        else:
            stream_class = stream_type.get_class()
        meta = self.get_compatible_meta(stream_class, ex=ex)
        if not save_name:
            meta.pop('name')
        if not save_count:
            meta.pop('count')
        meta.update(kwargs)
        if 'context' not in meta:
            meta['context'] = self.get_context()
        stream = sm.StreamType.of(stream_type).stream(data, **meta)
        return stream

    def map_to_type(self, function, stream_type=arg.DEFAULT) -> Stream:
        stream_type = arg.undefault(stream_type, self.get_stream_type())
        result = self.stream(
            map(function, self.get_items()),
            stream_type=stream_type,
        )
        if hasattr(self, 'is_in_memory'):
            if self.is_in_memory():
                return result.to_memory()
        return result

    # @deprecated_with_alternative('map_to_type()')
    def map_to_records(self, function) -> RecordStream:
        return self.map_to_type(
            function,
            stream_type=sm.StreamType.RecordStream,
        )

    # @deprecated_with_alternative('map_to_type()')
    def map_to_any(self, function) -> AnyStream:
        return self.map_to_type(
            function,
            stream_type=sm.StreamType.AnyStream,
        )

    def to_any_stream(self) -> AnyStream:
        return self.stream(
            self.get_items(),
            stream_type=sm.StreamType.AnyStream,
        )

    def to_line_stream(
            self,
            delimiter: Union[str, arg.DefaultArgument] = arg.DEFAULT,
            columns: Optional[Iterable] = arg.DEFAULT,
            add_title_row: Union[bool, arg.DefaultArgument] = arg.DEFAULT,
    ) -> LineStream:
        delimiter = arg.undefault(delimiter, '\t' if sm.StreamType.RowStream.isinstance(self) else None)
        stream = self
        if stream.get_stream_type() == sm.StreamType.RecordStream:
            assert isinstance(stream, sm.RecordStream)
            columns = arg.undefault(columns, stream.get_columns, delayed=True)
            add_title_row = arg.undefault(add_title_row, True)
            stream = stream.to_row_stream(columns=columns, add_title_row=add_title_row),
        if delimiter:
            func = delimiter.join
        else:
            func = str
        return self.stream(
            stream.get_mapped_items(func),
            stream_type=sm.StreamType.LineStream,
        )

    def to_json(self) -> LineStream:
        return self.stream(
            self.get_mapped_items(json.dumps),
            stream_type=sm.StreamType.LineStream,
        )

    def to_record_stream(self, *args, **kwargs) -> RecordStream:
        if 'function' in kwargs:
            func = kwargs.pop('function')
            items = self.get_mapped_items(lambda i: func(i, *args, **kwargs))
        elif 'columns' in kwargs and len(kwargs) == 1:
            columns = kwargs.pop('columns')
            assert not args
            items = self.get_records(columns)
        elif self.get_stream_type() == sm.StreamType.RecordStream:
            return self
        elif kwargs:  # and not args
            assert not args
            return self.to_any_stream().select(**kwargs)
        elif args:  # and not kwargs
            if len(kwargs) == 1:
                if callable(args[0]):
                    items = self.get_mapped_items(args[0])
                else:
                    items = self.get_records(args[0])
            else:
                items = self.get_records(args)
        else:  # not (args or kwargs):
            if hasattr(self, 'get_records'):
                items = self.get_records()
            else:
                items = self.get_mapped_items(lambda i: dict(item=i))
        return self.stream(
            items,
            stream_type=sm.StreamType.RecordStream,
        )

    def to_row_stream(self, *args, **kwargs) -> RowStream:
        function, delimiter = None, None
        if 'function' in kwargs:
            function = kwargs.pop('function')
        elif args:
            if callable(args[0]):
                function, args = args[0], args[1:]
            elif self.get_stream_type() in (sm.StreamType.LineStream, sm.StreamType.AnyStream):
                delimiter, args = args[0], args[1:]
        elif self.get_stream_type() == sm.StreamType.RecordStream:
            add_title_row = kwargs.pop('add_title_row', None)
            columns = arg.update(args, kwargs.pop('columns', None))
            assert isinstance(self, sm.RecordStream)
            if not columns:
                columns = self.get_columns()
            function = self.get_rows(columns=columns, add_title_row=add_title_row)
        elif 'delimiter' in kwargs and self.get_stream_type() in (sm.StreamType.LineStream, sm.StreamType.AnyStream):
            delimiter = kwargs.pop('delimiter')
        elif args:
            assert not kwargs
            return self.to_any_stream().select(*args)
        if function:
            items = self.get_mapped_items(lambda i: function(i, *args, **kwargs))
        elif delimiter:
            items = csv.reader(self.get_items(), *args, delimiter=delimiter, **kwargs)
        else:
            items = self.get_items()
        return self.stream(
            items,
            stream_type=sm.StreamType.RowStream,
        )

    def to_key_value_stream(self, key=fs.value_by_key(0), value=fs.value_by_key(1)) -> KeyValueStream:
        if isinstance(key, (list, tuple)):
            key = fs.composite_key(key)
        if isinstance(value, (list, tuple)):
            value = fs.composite_key(value)
        return self.stream(
            self.get_mapped_items(lambda i: (key(i), value(i))),
            stream_type=sm.StreamType.KeyValueStream,
        )

    # @deprecated_with_alternative('ConvertMixin.to_key_value_stream()')
    def to_pairs(self, *args, **kwargs) -> KeyValueStream:
        if sm.StreamType.LineStream:
            return self.to_row_stream(*args, **kwargs).to_key_value_stream()
        else:
            return self.to_key_value_stream(*args, **kwargs)

    def to_stream(self, stream_type=arg.DEFAULT, *args, **kwargs) -> Stream:
        stream_type = arg.undefault(stream_type, self.get_stream_type())
        method_suffix = sm.StreamType.of(stream_type).get_method_suffix()
        method_name = 'to_{}'.format(method_suffix)
        stream_method = self.__getattribute__(method_name)
        return stream_method(stream_type, *args, **kwargs)
