from abc import ABC
from typing import Optional, Callable, Iterable, Iterator, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        Stream, LineStream, RowStream, RecordStream, KeyValueStream, StructStream,
        StreamType, ItemType, Item, LoggingLevel,
        StructInterface, FieldInterface, FieldName, OptionalFields, UniKey,
        Columns, Class, Array, ARRAY_TYPES,
        ROW_SUBCLASSES, RECORD_SUBCLASSES,
    )
    from base.constants.chars import TAB_CHAR
    from base.functions.arguments import get_names
    from base.functions.errors import get_type_err_msg, get_loc_message
    from content.items.simple_items import FULL_ITEM_FIELD, MutableRecord, MutableRow, ImmutableRow, SimpleRow
    from content.struct.flat_struct import FlatStruct
    from functions.secondary import all_secondary_functions as fs
    from utils.external import pd, DataFrame
    from utils.decorators import deprecated_with_alternative
    from streams.stream_builder import StreamBuilder
    from streams.interfaces.regular_stream_interface import RegularStreamInterface
    from streams.abstract.iterable_stream import IterableStream
    from streams.mixin.validate_mixin import ValidateMixin
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        Stream, LineStream, RowStream, RecordStream, KeyValueStream, StructStream,
        StreamType, ItemType, Item, LoggingLevel,
        StructInterface, FieldInterface, FieldName, OptionalFields, UniKey,
        Columns, Class, Array, ARRAY_TYPES,
        ROW_SUBCLASSES, RECORD_SUBCLASSES,
    )
    from ...base.constants.chars import TAB_CHAR
    from ...base.functions.arguments import get_names
    from ...base.functions.errors import get_type_err_msg, get_loc_message
    from ...content.items.simple_items import FULL_ITEM_FIELD, MutableRecord, MutableRow, ImmutableRow, SimpleRow
    from ...content.struct.flat_struct import FlatStruct
    from ...functions.secondary import all_secondary_functions as fs
    from ...utils.external import pd, DataFrame
    from ...utils.decorators import deprecated_with_alternative
    from ..stream_builder import StreamBuilder
    from ..interfaces.regular_stream_interface import RegularStreamInterface
    from ..abstract.iterable_stream import IterableStream
    from .validate_mixin import ValidateMixin

Native = RegularStreamInterface
AnyStream = Stream
StructOrColumns = Union[StructInterface, Columns, FieldInterface, FieldName, None]
OptionalArguments = Union[str, Iterable, None]

DEFAULT_FIELDS_COUNT = 99
DEFAULT_COL_DELIMITER = TAB_CHAR
DEFAULT_COL_MASK = 'column{n:02}'
STRUCTURED_ITEM_TYPES = ItemType.Record, ItemType.Row
UNSTRUCTURED_ITEM_TYPES = ItemType.Line, ItemType.Any, ItemType.Auto


class ConvertMixin(IterableStream, ValidateMixin, ABC):
    def get_items(self, item_type: ItemType = ItemType.Auto) -> Iterable:
        if item_type not in (ItemType.Auto, None):
            return self.get_items_of_type(item_type)
        else:
            return self.get_stream_data()

    def get_items_of_type(self, item_type: ItemType) -> Iterator:
        err_msg = 'StructStream.get_items_of_type(item_type): Expected StructRow, Row, Record, got item_type={}'
        struct = self._get_struct()
        columns = list(self._get_columns())
        for i in self.get_stream_data():
            if isinstance(i, ROW_SUBCLASSES):
                if item_type == ItemType.Row:
                    yield i
                elif item_type == ItemType.Record:
                    yield {k: v for k, v in zip(columns, i)}
                elif item_type == ItemType.Line:
                    if isinstance(struct, FlatStruct):
                        yield struct.format(i)
                    else:
                        yield DEFAULT_COL_DELIMITER.join(i)
                else:
                    raise ValueError(err_msg.format(item_type))
            elif isinstance(i, RECORD_SUBCLASSES):
                struct = self._get_struct()
                if item_type == ItemType.Row:
                    yield [i.get(c) for c in columns]
                elif item_type == ItemType.Record:
                    yield i
                elif item_type == ItemType.Line:
                    if isinstance(struct, FlatStruct):
                        yield struct.format(i)
                    else:
                        row = [i.get(c) for c in columns]
                        yield DEFAULT_COL_DELIMITER.join(row)
                else:
                    raise ValueError(err_msg.format(item_type))
            else:
                message = get_type_err_msg(i, expected=ROW_SUBCLASSES, arg='item', kwargs=dict(item_type=item_type))
                raise TypeError(message)

    def get_lines(self) -> Iterable[str]:
        return self.get_items_of_type(ItemType.Line)

    def get_rows(self, columns: StructOrColumns = None) -> Iterator[SimpleRow]:
        if isinstance(self, RegularStreamInterface) or hasattr(self, 'get_item_type'):
            item_type = self.get_item_type()
        else:
            item_type = ItemType.Auto
        if columns:
            key_function = fs.composite_key(columns)
            return self._get_mapped_items(key_function)
        else:
            if item_type in (ItemType.Any, ItemType.Auto, None):
                example_item = self.get_one_item()
                item_type = ItemType.detect(example_item)
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
            elif item_type == ItemType.Line:
                delimiter = columns
                assert isinstance(delimiter, str), f'LineStream.get_rows(): expected delimiter as str, got {delimiter}'
                for i in self.get_items():
                    yield i.split(delimiter)
            else:
                raise ValueError(f'ConvertMixin.get_rows(): item type {item_type} not supported.')

    def get_records(self, columns: StructOrColumns = None) -> Iterable:
        item_type = self.get_item_type()
        assert isinstance(item_type, ItemType) or hasattr(item_type, 'get_field_getter')
        columns = self._get_columns(columns)
        if item_type == ItemType.Record:
            if columns:
                return self.select(*columns).get_items()
            else:
                return self.get_items()
        if item_type == ItemType.Row:
            if columns:
                func = (lambda r: {k: v for k, v in zip(columns, r)})
            else:
                func = (lambda r: {DEFAULT_COL_MASK.format(n + 1): v for n, v in enumerate(r)})
        elif item_type == ItemType.Line:
            func = fs.json_loads(default=dict(_err='JSONDecodeError'), skip_errors=True)
        elif item_type in UNSTRUCTURED_ITEM_TYPES:
            single_field_name = columns[0]
            func = (lambda i: {single_field_name: i})
        else:
            raise TypeError(f'ConvertMixin.get_records(): item_type={item_type} not supported')
        return self._get_mapped_items(func)

    def get_struct_rows(
            self,
            struct: Optional[StructInterface] = None,
            skip_bad_rows: bool = False,
            skip_bad_values: bool = False,
            skip_missing: bool = False,
            verbose: bool = True,
            inplace: bool = False,
    ) -> Iterator[SimpleRow]:
        rows = self.get_rows(columns=struct)
        if struct is None:
            struct = self.get_struct()
        if isinstance(struct, StructInterface) or hasattr(struct, 'get_converters'):
            converters = struct.get_converters(src='str', dst='py')
            converted_row = list()
            for r in rows:
                if not inplace:
                    converted_row = list()
                for col, (value, converter) in enumerate(zip(r, converters)):
                    if converter:
                        try:
                            converted_value = converter(value)
                        except TypeError as e:
                            if skip_bad_rows:
                                converted_row = None
                                break
                            elif skip_bad_values:
                                converted_value = None
                            else:
                                raise e
                            if verbose:
                                msg = f'get_struct_rows() can not {converter}({value}) in {r}: {e}'
                                self.log(msg=msg, level=LoggingLevel.Warning)
                    else:
                        converted_value = value
                    if inplace:
                        r[col] = converted_value
                    else:
                        converted_row.append(converted_value)
                if inplace:
                    yield r
                elif converted_row is not None:
                    yield converted_row.copy()
        elif skip_missing:
            yield from rows
        else:
            raise TypeError(f'get_struct_rows(): Expected struct as StructInterface, got {struct}')

    def get_struct_records(
            self,
            struct: Optional[StructInterface] = None,
            skip_bad_rows: bool = False,
            skip_bad_values: bool = False,
            skip_missing: bool = False,
            verbose: bool = True,
            inplace: bool = True,
    ) -> Iterator[MutableRecord]:
        records = self.get_records(columns=struct)
        if struct is None:
            struct = self.get_struct()
        columns = self._get_columns(struct)
        if isinstance(struct, StructInterface) or hasattr(struct, 'get_converters'):
            converters = struct.get_converters(src='str', dst='py')
            for r in records:
                if inplace:
                    converted_record = r
                else:
                    converted_record = MutableRecord()
                for field, value, converter in zip(columns, r, converters):
                    try:
                        converted_value = converter(value)
                    except TypeError as e:
                        if skip_bad_rows:
                            converted_record = None
                            break
                        elif skip_bad_values:
                            converted_value = None
                        else:
                            raise e
                        if verbose:
                            msg = f'get_struct_rows() can not {converter}({value}) in {r}: {e}'
                            self.log(msg=msg, level=LoggingLevel.Warning)
                    converted_record[field] = converted_value
                if converted_record is not None:
                    yield converted_record
        elif skip_missing:
            yield from records
        else:
            raise TypeError(f'get_struct_records(): Expected struct as StructInterface, got {struct}')

    def structure(
            self,
            struct: Optional[StructInterface] = None,
            skip_bad_rows: bool = False,
            skip_bad_values: bool = False,
            skip_missing: bool = False,
            verbose: bool = True,
            inplace: Optional[bool] = None,
    ) -> Native:
        if struct is None:
            struct = self.get_struct()
        item_type = self.get_item_type()
        if item_type == ItemType.Record:
            f = self.get_struct_records
        elif item_type == ItemType.Row:
            f = self.get_struct_rows
        elif skip_missing:
            return self._assume_native(self)
        else:
            raise TypeError(f'structure() can apply struct only for Rows and Records, got {item_type}')
        data = f(struct, skip_bad_rows=skip_bad_rows, skip_bad_values=skip_bad_values, verbose=verbose, inplace=inplace)
        count = None if skip_bad_rows else self.get_count()
        stream = self.stream(data, struct=struct, count=count, check=False)
        if self.is_in_memory():
            stream = stream.collect()
        return stream

    def get_dataframe(self, columns: Columns = None) -> DataFrame:
        if columns and hasattr(self, 'select'):
            data = self.select(*columns).get_data()
        else:
            data = self.get_data()
        if DataFrame:
            return DataFrame(data)

    def _get_columns(self, columns: StructOrColumns = None) -> Optional[list]:
        struct = self._get_struct(columns)
        if isinstance(struct, StructInterface) or hasattr(struct, 'get_columns'):
            columns = struct.get_columns()
        elif isinstance(struct, Iterable):
            columns = list(struct)
        else:
            raise TypeError(f'Expected struct as Struct, got {struct}')
        if columns:
            if self.get_item_type() in UNSTRUCTURED_ITEM_TYPES:
                assert len(columns) == 1
            return columns

    def _get_struct(self, struct: StructOrColumns = None) -> Optional[StructInterface]:
        is_one_field = isinstance(struct, (FieldInterface, FieldName)) or hasattr(struct, 'get_value_type')
        if is_one_field:
            struct = [struct]
        default_struct = self.get_struct()
        if struct is not None:
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
        elif default_struct is not None:
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
                    if example_item:
                        columns = list(example_item.keys())
                    else:
                        columns = list()
                elif item_type == ItemType.Row:
                    columns = [DEFAULT_COL_MASK.format(n + 1) for n in range(fields_count)]
                else:
                    raise TypeError(f'Expected {STRUCTURED_ITEM_TYPES}, got {item_type}')
            elif item_type in UNSTRUCTURED_ITEM_TYPES:
                columns = [FULL_ITEM_FIELD]
            else:
                supported_item_types = ItemType.Row, *UNSTRUCTURED_ITEM_TYPES
                raise TypeError(f'Expected one of {supported_item_types}, got {item_type}')
            return FlatStruct(columns)

    def _get_stream_type(self, item_type: ItemType = ItemType.Auto) -> StreamType:
        if item_type not in (ItemType.Auto, None):
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

    def _get_item_type(self, item_type: ItemType = ItemType.Auto) -> ItemType:
        if item_type in (ItemType.Auto, None):
            return self.get_item_type()
        else:
            if isinstance(item_type, str):
                try:
                    item_type = ItemType(item_type)
                except ValueError:  # stream_type is not a valid StreamType
                    item_type = StreamType(item_type)
            if isinstance(item_type, ItemType) or hasattr(item_type, 'get_field_getter'):
                return item_type
            elif isinstance(item_type, StreamType):  # deprecated
                return item_type.get_item_type()
            elif isinstance(item_type, RegularStreamInterface) or hasattr(item_type, 'get_item_type'):
                return item_type.get_item_type()
            else:
                raise TypeError(f'ConvertMixin._get_stream_type(): Expected ItemType or StreamType, got {item_type}')

    def _get_mapped_items(self, function: Callable, flat: bool = False, skip_errors: bool = False) -> Iterator[Item]:
        if skip_errors:
            logger = self.get_selection_logger()
            if flat:
                for i in self.get_items():
                    try:
                        yield from function(i)
                    except (ValueError, TypeError) as e:
                        logger.log_selection_error(function, in_fields=['*'], in_values=[i], in_record=i, message=e)
            else:
                yield from map(function, self.get_items())
        else:
            yield from super()._get_mapped_items(function, flat=flat)

    def stream(
            self,
            data: Iterable,
            item_type: ItemType = ItemType.Auto,
            ex: OptionalArguments = None,
            save_name: bool = True,
            save_count: bool = True,
            **kwargs
    ) -> Stream:
        if item_type not in (ItemType.Any, None):
            given_item_type = self._get_item_type(item_type)
            if 'stream_type' in kwargs:
                given_stream_type = kwargs['stream_type']
                msg = 'stream_type argument is deprecated, use item_type instead'
                self.log(get_loc_message(msg, self.stream, args=[item_type], kwargs=kwargs), level=LoggingLevel.Warning)
                expected_item_type = self._get_item_type(given_stream_type)
                assert given_item_type == expected_item_type, f'{given_item_type} vs {expected_item_type}'
            else:
                kwargs['item_type'] = given_item_type
        meta = self.get_meta()
        if not save_name:
            meta.pop('name')
        if not save_count:
            meta.pop('count')
        meta.update(kwargs)
        if 'context' not in meta and hasattr(self, 'get_context'):
            meta['context'] = self.get_context()
        if 'value_item_type' in meta:
            meta.pop('value_item_type')  # unify KeyValueStream to RegularStream
        stream = StreamBuilder.stream(data, **meta)
        return stream

    def map_to_type(self, function: Callable, item_type: ItemType = ItemType.Auto) -> Stream:
        if item_type in (ItemType.Auto, None):
            item_type = self.get_item_type()
        items = map(function, self.get_items())
        result = self.stream(items, item_type=item_type)
        if hasattr(self, 'is_in_memory'):
            if self.is_in_memory():
                return result.to_memory()
        return result

    @deprecated_with_alternative('map_to_type(item_type=ItemType.Records)')
    def map_to_records(self, function: Callable) -> RecordStream:
        stream = self.map_to_type(function, item_type=ItemType.Record)
        return self._assume_native(stream)

    @deprecated_with_alternative('map_to_type(item_type=ItemType.Any)')
    def map_to_any(self, function: Callable) -> AnyStream:
        return self.map_to_type(function, item_type=ItemType.Any)

    @deprecated_with_alternative('map_to_type(item_type=ItemType.Any)')
    def to_any_stream(self) -> AnyStream:
        return self.stream(self.get_items(), item_type=ItemType.Any)

    def to_line_stream(
            self,
            delimiter: Optional[str] = None,
            columns: Columns = None,
            add_title_row: Optional[bool] = None,
    ) -> LineStream:
        item_type = self.get_item_type()
        if delimiter is None:
            delimiter = DEFAULT_COL_DELIMITER if item_type == ItemType.Row else None
        stream = self
        if item_type == ItemType.Record:
            assert isinstance(stream, RegularStreamInterface) or hasattr(stream, 'get_columns'), 'got {}'.format(stream)
            if not columns:
                columns = stream.get_columns()
            if add_title_row is None:
                add_title_row = True
            stream = stream.to_row_stream(columns=columns, add_title_row=add_title_row)
        elif item_type == ItemType.Row:
            if columns:
                stream = self.select(columns)
        if delimiter is not None:
            func = fs.csv_dumps(delimiter)
        else:
            func = str
        lines = stream._get_mapped_items(func)
        stream = self.stream(lines, item_type=ItemType.Line)
        return self._assume_native(stream)

    def to_json(self, *args, **kwargs) -> LineStream:
        items = self._get_mapped_items(fs.json_dumps(*args, **kwargs))
        stream = self.stream(items, item_type=ItemType.Line)
        return self._assume_native(stream)

    def to_record_stream(self, *args, **kwargs) -> RecordStream:
        if 'function' in kwargs:
            func = kwargs.pop('function')
            items = self._get_mapped_items(lambda i: func(i, *args, **kwargs))
        elif 'columns' in kwargs and len(kwargs) == 1:
            columns = kwargs.pop('columns')
            assert not args
            items = self.get_records(columns)
        elif self.get_item_type() == ItemType.Record:
            return self
        elif kwargs:  # and not args
            assert not args
            return self.select(**kwargs)
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
        stream = self.stream(items, item_type=ItemType.Record)
        return self._assume_native(stream)

    def to_row_stream(
            self,
            arg: OptionalArguments = None,
            columns: StructOrColumns = None,
            delimiter: Optional[str] = None,
    ) -> RowStream:
        args_str = f'arg={repr(arg)}, columns={repr(columns)}, delimiter={repr(delimiter)}'
        msg = f'to_row_stream(): only one of this args allowed: {args_str}'
        item_type = self.get_item_type()
        func = None
        if arg:
            assert not delimiter, msg
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
            if not columns:
                columns = self.get_columns()
        elif item_type == ItemType.Line:
            if delimiter is None:
                delimiter = DEFAULT_COL_DELIMITER  # '\t'
        if delimiter:
            assert item_type == ItemType.Line
            assert isinstance(delimiter, str), f'to_row_stream(): Expected delimiter as str, got {delimiter}'
            assert not columns, f'got {columns}'
            assert not func, msg
            func = fs.csv_loads(delimiter=delimiter)
        if columns:
            if not func:
                func = fs.composite_key(*columns, item_type=item_type)
            struct = self._get_struct(columns)
        else:
            struct = self.get_struct()
        if func:
            items = self._get_mapped_items(func)
        else:
            items = self.get_items()
        stream = self.stream(items, item_type=ItemType.Row, struct=struct)
        return self._assume_native(stream)

    def to_key_value_stream(
            self,
            key: UniKey = fs.first(),
            value: UniKey = fs.second(),
            skip_errors: bool = False,
    ) -> KeyValueStream:
        key_func = self._get_key_function(key, take_hash=False)
        if value:
            value_func = self._get_key_function(value, take_hash=False)
        else:
            value_func = fs.same()
        items = self._get_mapped_items(lambda i: (key_func(i), value_func(i)), skip_errors=skip_errors)
        if self.is_in_memory():
            items = list(items)
        stream = self.stream(items, item_type=ItemType.Row, check=False)
        return self._assume_native(stream)

    # @deprecated_with_alternative('ConvertMixin.to_key_value_stream()')
    def to_pairs(self, *args, **kwargs) -> KeyValueStream:
        if self.get_item_type() == ItemType.Line:
            return self.to_row_stream(*args, **kwargs).to_key_value_stream()
        else:
            return self.to_key_value_stream(*args, **kwargs)

    # @deprecated
    def to_stream(self, item_type: ItemType = ItemType.Auto, *args, **kwargs) -> Stream:
        assert not args, 'ConvertMixin.to_stream(): unnamed ordered args not supported'
        return self.stream(self.get_data(), item_type=item_type, **kwargs)

    @staticmethod
    def _assume_native(stream) -> Native:
        return stream
