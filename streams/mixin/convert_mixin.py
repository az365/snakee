from abc import ABC
from typing import Optional, Callable, Iterable, Iterator, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        Stream, RegularStream, LineStream, RowStream, RecordStream, KeyValueStream, StructStream,
        StreamType, ItemType,
        AUTO, Auto, AutoColumns, Columns, Array, OptionalFields, FieldName, FieldInterface, StructInterface,
    )
    from base.functions.arguments import get_names, get_list, update
    from base.constants.chars import TAB_CHAR
    from content.items.simple_items import FULL_ITEM_FIELD, MutableRecord, MutableRow, ImmutableRow, SimpleRow
    from content.struct.flat_struct import FlatStruct
    from content.struct.struct_row import StructRow, StructRowInterface, ROW_SUBCLASSES
    from functions.secondary import all_secondary_functions as fs
    from utils.external import pd, DataFrame
    from streams.interfaces.regular_stream_interface import RegularStreamInterface
    from streams.abstract.iterable_stream import IterableStream
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        Stream, RegularStream, LineStream, RowStream, RecordStream, KeyValueStream, StructStream,
        StreamType, ItemType,
        AUTO, Auto, AutoColumns, Columns, Array, OptionalFields, FieldName, FieldInterface, StructInterface,
    )
    from ...base.functions.arguments import get_names, get_list, update
    from ...base.constants.chars import TAB_CHAR
    from ...content.items.simple_items import FULL_ITEM_FIELD, MutableRecord, MutableRow, ImmutableRow, SimpleRow
    from ...content.struct.flat_struct import FlatStruct
    from ...content.struct.struct_row import StructRow, StructRowInterface, ROW_SUBCLASSES
    from ...functions.secondary import all_secondary_functions as fs
    from ...utils.external import pd, DataFrame
    from ..interfaces.regular_stream_interface import RegularStreamInterface
    from ..abstract.iterable_stream import IterableStream

Native = RegularStream
AnyStream = Stream
AutoStreamType = Union[Auto, StreamType]  # deprecated, use AutoItemType instead
StreamItemType = Union[StreamType, ItemType, Auto]
StructOrColumns = Union[StructInterface, AutoColumns, FieldInterface, FieldName]
OptionalArguments = Optional[Union[str, Iterable]]

DEFAULT_DELIMITER = TAB_CHAR
DEFAULT_FIELDS_COUNT = 99
DEFAULT_COL_MASK = 'column{n:02}'
STRUCTURED_ITEM_TYPES = ItemType.Record, ItemType.Row, ItemType.StructRow
UNSTRUCTURED_ITEM_TYPES = ItemType.Line, ItemType.Any, ItemType.Auto


class ConvertMixin(IterableStream, ABC):
    def get_items(self, item_type: Union[ItemType, Auto] = AUTO) -> Iterable:
        if Auto.is_defined(item_type):
            return self.get_items_of_type(item_type)
        else:
            return self.get_stream_data()

    def get_items_of_type(self, item_type: ItemType) -> Iterator:
        err_msg = 'StructStream.get_items_of_type(item_type): Expected StructRow, Row, Record, got item_type={}'
        columns = list(self.get_columns())
        for i in self.get_stream_data():
            if isinstance(i, StructRowInterface) or hasattr(i, 'get_data'):
                if item_type == ItemType.StructRow:
                    yield i
                elif item_type == ItemType.Row:
                    yield i.get_data()
                elif item_type == ItemType.Record:
                    yield {k: v for k, v in zip(columns, i.get_data())}
                else:
                    raise ValueError(err_msg.format(item_type))
            elif isinstance(i, ROW_SUBCLASSES):
                if item_type == ItemType.Row:
                    yield i
                elif item_type == ItemType.StructRow:
                    yield StructRow(i, self._get_struct())
                elif item_type == ItemType.Record:
                    yield {k: v for k, v in zip(columns, i)}
                else:
                    raise ValueError(err_msg.format(item_type))
            else:
                msg = 'StructStream.get_items_of_type(item_type={}): Expected items as Row or StructRow, got {} as {}'
                raise TypeError(msg.format(item_type, i, type(i)))

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
            elif item_type == ItemType.Line:
                delimiter = columns
                for i in self.get_items():
                    yield i.split(delimiter)
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
        assert isinstance(item_type, ItemType)
        columns = self._get_columns(columns)
        if item_type == ItemType.Record:
            if Auto.is_defined(columns):
                return self.select(*columns).get_items()
            else:
                return self.get_items()
        if item_type == ItemType.Row:
            if Auto.is_defined(columns):
                func = (lambda r: {k: v for k, v in zip(columns, r)})
            else:
                func = (lambda r: {DEFAULT_COL_MASK.format(n + 1): v for n, v in enumerate(r)})
        elif item_type in UNSTRUCTURED_ITEM_TYPES:
            func = (lambda i: {columns[0]: i})
        else:
            raise TypeError(f'ConvertMixin.get_records(): item_type={item_type} not supported')
        return self._get_mapped_items(func)

    def get_dataframe(self, columns: Columns = None) -> DataFrame:
        if columns and hasattr(self, 'select'):
            data = self.select(*columns).get_data()
        else:
            data = self.get_data()
        if DataFrame:
            return DataFrame(data)

    def _get_columns(self, columns: StructOrColumns = AUTO) -> Optional[list]:
        struct = self._get_struct(columns)
        if isinstance(struct, StructInterface) or hasattr(struct, 'get_columns'):
            columns = struct.get_columns()
        elif isinstance(struct, Iterable):
            columns = list(struct)
        else:
            raise TypeError(f'Expected struct as Struct, got {struct}')
        if Auto.is_defined(columns):
            if self.get_item_type() in UNSTRUCTURED_ITEM_TYPES:
                assert len(columns) == 1
            return columns

    def _get_struct(
            self,
            struct: StructOrColumns = AUTO,
    ) -> Optional[StructInterface]:
        is_one_field = isinstance(struct, (FieldInterface, FieldName)) or hasattr(struct, 'get_value_type')
        if is_one_field:
            struct = [struct]
        default_struct = self.get_struct()
        if Auto.is_defined(struct):
            if isinstance(struct, StructInterface) or hasattr(struct, 'get_fields'):
                return struct
            elif isinstance(struct, Iterable):  # isinstance(struct, Columns)
                if isinstance(default_struct, StructInterface) or hasattr(default_struct, 'get_fields'):
                    default_fields = default_struct.get_field_names()
                    default_types = default_struct.get_types_dict()
                    refined_struct = FlatStruct()
                    for f in struct:
                        if isinstance(f, FieldInterface) or hasattr(f, 'get_value_type'):
                            refined_struct.append(f)
                        elif isinstance(f, FieldName):
                            if f in default_fields:
                                refined_struct.add_fields(f, default_type=default_types.get(f))
                        else:
                            raise TypeError(f'expected field as Field, got {f}')
                    return refined_struct
                else:
                    return FlatStruct(struct)
            else:
                raise TypeError(f'expected struct as FlatStruct or Columns, got {struct}')
        elif Auto.is_defined(default_struct):
            return default_struct
        else:
            item_type = self.get_item_type()
            if item_type in STRUCTURED_ITEM_TYPES:
                example_item = self.get_one_item()
                if example_item:
                    fields_count = len(example_item)
                else:
                    fields_count = DEFAULT_FIELDS_COUNT
                if item_type == ItemType.Record:
                    columns = list(example_item.keys())
                elif item_type == ItemType.Row:
                    columns = [DEFAULT_COL_MASK.format(n + 1) for n in range(fields_count)]
                elif item_type == ItemType.StructRow:
                    try:
                        columns = example_item.get_columns()
                    except TypeError:
                        columns = [DEFAULT_COL_MASK.format(n + 1) for n in range(fields_count)]
                else:
                    raise TypeError(f'Expected {STRUCTURED_ITEM_TYPES}, got {item_type}')
            elif item_type in UNSTRUCTURED_ITEM_TYPES:
                columns = [FULL_ITEM_FIELD]
            else:
                supported_item_types = ItemType.Row, *UNSTRUCTURED_ITEM_TYPES
                raise TypeError(f'Expected one of {supported_item_types}, got {item_type}')
            return FlatStruct(columns)

    def _get_stream_type(self, item_type: Union[ItemType, StreamType, Auto, None] = None) -> StreamType:
        if Auto.is_defined(item_type):
            if isinstance(item_type, str):
                try:
                    item_type = ItemType(item_type, default=None)
                except ValueError:  # stream_type is not a valid StreamType
                    item_type = StreamType(item_type)
            if isinstance(item_type, ItemType):
                item_type_name = item_type.get_name()
                stream_type_name = f'{item_type_name}Stream'
                return StreamType(stream_type_name)
            elif isinstance(item_type, StreamType):
                return item_type
            elif isinstance(item_type, RegularStreamInterface) or hasattr(item_type, 'get_stream_type'):
                return item_type.get_stream_type()
            else:
                raise TypeError(f'ConvertMixin._get_stream_type(): Expected ItemType or StreamType, got {item_type}')
        else:
            return self.get_stream_type()

    def stream(
            self,
            data: Iterable,
            stream_type: StreamItemType = AUTO,  # deprecated argument
            ex: OptionalArguments = None,
            save_name: bool = True,
            save_count: bool = True,
            **kwargs
    ) -> Stream:
        if Auto.is_defined(stream_type):
            stream_type = self._get_stream_type(stream_type)
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
                items = self._get_mapped_items(lambda i: {FULL_ITEM_FIELD: i})
        stream = self.stream(items, stream_type=StreamType.RecordStream)
        return self._assume_native(stream)

    def to_row_stream(
            self,
            arg: Union[str, Iterable, None] = None,
            columns: Union[Iterable, Auto] = AUTO,
            delimiter: Union[str, Auto] = AUTO,
    ) -> RowStream:
        args_str = f'arg={repr(arg)}, columns={repr(columns)}, delimiter={repr(delimiter)}'
        msg = f'to_row_stream(): only one of this args allowed: {args_str}'
        item_type = self.get_item_type()
        func = None
        if arg:
            assert not Auto.is_defined(delimiter), msg
            if isinstance(arg, Callable):
                func = arg
            elif isinstance(arg, str):
                if item_type == ItemType.Line:
                    delimiter = arg
                else:  # Row, Record, ...
                    columns = [arg]
            elif isinstance(arg, Iterable):
                columns = arg
            else:
                raise TypeError(f'ConvertMixin.to_row_stream(): Expected function, column(s) or delimiter, got {arg}')
        if item_type == ItemType.Record:
            if not Auto.is_defined(columns):
                columns = self.get_columns()
        elif item_type == ItemType.Line:
            if not Auto.is_defined(delimiter):
                delimiter = DEFAULT_DELIMITER  # '\t'
        if Auto.is_defined(delimiter):
            assert item_type == ItemType.Line
            assert not Auto.is_defined(columns), f'got {columns}'
            assert not func, msg
            func = fs.csv_loads(delimiter=delimiter)
        if Auto.is_defined(columns):
            if not func:
                func = fs.composite_key(*columns, item_type=item_type)
            struct = columns
        else:
            struct = self.get_struct()
        if func:
            items = self._get_mapped_items(func)
        else:
            items = self.get_items()
        stream = self.stream(items, stream_type=StreamType.RowStream, struct=struct)
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
