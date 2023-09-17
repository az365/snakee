from typing import Union, Iterable, Iterator, Generator, Sequence, Callable, Optional
from inspect import isclass

try:  # Assume we're a submodule in a package.
    from interfaces import (
        StructInterface, FieldInterface, LeafConnectorInterface,
        Stream, Context, Source, TmpFiles,
        StreamType, ItemType, ValueType, JoinType, How, LoggingLevel,
        Count, Item, Struct, Columns, Field, FieldNo, OptionalFields, UniKey, Class,
        Name, Array, ARRAY_TYPES,
    )
    from base.constants.chars import EMPTY, SHARP
    from base.functions.arguments import get_name, get_names, get_str_from_args_kwargs
    from base.functions.errors import get_type_err_msg
    from utils.decorators import deprecated_with_alternative
    from functions.primary.items import set_to_item, merge_two_items, unfold_structs_to_fields
    from functions.secondary import all_secondary_functions as fs
    from content.items.item_getters import get_filter_function
    from content.selection import selection_classes as sn
    from content.struct.struct_mixin import StructMixin
    from content.struct.flat_struct import FlatStruct
    from streams.abstract.local_stream import LocalStream
    from streams.interfaces.abstract_stream_interface import DEFAULT_EXAMPLE_COUNT
    from streams.interfaces.regular_stream_interface import RegularStreamInterface, DEFAULT_ANALYZE_COUNT
    from streams.mixin.convert_mixin import ConvertMixin
    from streams.stream_builder import StreamBuilder
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        StructInterface, FieldInterface, LeafConnectorInterface,
        Stream, Context, Source, TmpFiles,
        StreamType, ItemType, ValueType, JoinType, How, LoggingLevel,
        Count, Item, Struct, Columns, Field, FieldNo, OptionalFields, UniKey, Class,
        Name, Array, ARRAY_TYPES,
    )
    from ...base.constants.chars import EMPTY, SHARP
    from ...base.functions.arguments import get_name, get_names, get_str_from_args_kwargs
    from ...base.functions.errors import get_type_err_msg
    from ...utils.decorators import deprecated_with_alternative
    from ...functions.primary.items import set_to_item, merge_two_items, unfold_structs_to_fields
    from ...functions.secondary import all_secondary_functions as fs
    from ...content.items.item_getters import get_filter_function
    from ...content.selection import selection_classes as sn
    from ...content.struct.struct_mixin import StructMixin
    from ...content.struct.flat_struct import FlatStruct
    from ..abstract.local_stream import LocalStream
    from ..interfaces.abstract_stream_interface import DEFAULT_EXAMPLE_COUNT
    from ..interfaces.regular_stream_interface import RegularStreamInterface, DEFAULT_ANALYZE_COUNT
    from ..mixin.convert_mixin import ConvertMixin
    from ..stream_builder import StreamBuilder

Native = Union[LocalStream, RegularStreamInterface]
FileObj = LeafConnectorInterface
FileName = str

DYNAMIC_META_FIELDS = 'struct', 'count', 'less_than'


class RegularStream(LocalStream, ConvertMixin, StructMixin, RegularStreamInterface):
    def __init__(
            self,
            data: Iterable[Item],
            name: Optional[Name] = None,
            caption: str = EMPTY,
            item_type: ItemType = ItemType.Any,
            struct: Struct = None,
            source: Source = None,
            context: Context = None,
            count: Count = None,
            less_than: Count = None,
            max_items_in_memory: Count = None,
            tmp_files: TmpFiles = None,
            check: bool = False,
    ):
        if struct and not isinstance(struct, (FlatStruct, StructInterface)):
            struct = FlatStruct(struct)
        self._struct = struct
        self._item_type = item_type
        if check:
            data = self._get_validated_items(data, context=context)
        super().__init__(
            data=data, check=check,
            name=name, caption=caption,
            source=source, context=context,
            count=count, less_than=less_than,
            max_items_in_memory=max_items_in_memory,
            tmp_files=tmp_files,
        )

    @staticmethod
    def _get_dynamic_meta_fields() -> tuple:
        return DYNAMIC_META_FIELDS

    @staticmethod
    def get_stream_class() -> Class:
        return RegularStream

    def get_item_type(self) -> ItemType:
        return self._item_type

    def set_item_type(self, item_type: ItemType, inplace: bool = True) -> Native:
        if inplace:
            self._set_item_type_inplace(item_type)
            return self
        else:
            stream = self.stream(self.get_data(), item_type=item_type)
            return self._assume_native(stream)

    def _set_item_type_inplace(self, item_type: ItemType) -> None:
        self._item_type = item_type

    item_type = property(get_item_type, _set_item_type_inplace)

    def get_struct_from_source(
            self,
            set_struct: bool = False,
            verbose: bool = False,
            skip_missing: bool = False,
    ) -> Struct:
        record = self.to_record_stream().get_one_item()
        if record is None and not skip_missing:
            raise ValueError('Can not detect struct from empty stream')
        struct = FlatStruct.get_struct_detected_by_record(record)
        if set_struct:
            self.set_struct(struct, check=False, inplace=True)
        return struct

    def get_initial_struct(self) -> Struct:
        return self._struct

    def get_struct(self) -> Struct:
        return self.get_initial_struct()

    def set_struct(self, struct: Struct, check: bool = False, inplace: bool = False) -> Native:
        if inplace:
            self._set_struct_inplace(struct)
            if check:
                self._get_validated_items(self.get_items())
            return self
        else:
            items = self.get_items()
            if check:
                items = self._get_validated_items(self.get_items(), struct=struct)
            stream = self.stream(items, struct=struct)
            return self._assume_native(stream)

    def _set_struct_inplace(self, struct: Struct) -> None:
        self._struct = struct

    struct = property(get_struct, _set_struct_inplace)

    def get_columns(self) -> Optional[list]:
        declared_columns = self.get_declared_columns()
        detected_columns = self.get_detected_columns()
        if declared_columns:
            if set(declared_columns) == set(detected_columns):
                return declared_columns
            else:
                columns = declared_columns.copy()
                for c in detected_columns:
                    if c not in columns:
                        columns.append(c)
                return columns
        else:
            return list(detected_columns)

    def get_detected_columns(
            self,
            by_items_count: int = DEFAULT_ANALYZE_COUNT,
            sort: bool = True,
            get_max: bool = True,
            skip_errors: bool = False,
    ) -> Sequence:
        item_type = self.get_item_type()
        if item_type in (ItemType.Any, ItemType.Line):
            return [self.get_item_type().get_value()]
        example = self.take(by_items_count)
        if item_type == ItemType.Record:
            columns = set()
            for r in example.get_items():
                columns.update(r.keys())
            if sort:
                columns = sorted(columns)
            return columns
        elif item_type == ItemType.Row:
            detected_row_len = 0
            for row in example.get_items():
                cur_row_len = len(row)
                if get_max:
                    if cur_row_len > detected_row_len:
                        detected_row_len = cur_row_len
                else:  # elif get_min:
                    if cur_row_len < detected_row_len:
                        detected_row_len = cur_row_len
            return range(detected_row_len)
        elif not skip_errors:
            raise NotImplementedError(item_type)

    def get_declared_columns(self) -> Optional[list]:
        struct = self.get_struct()
        if struct:
            return self.get_struct().get_columns()

    def get_column_count(self) -> int:
        return len(self.get_columns())

    def add_column(self, name: Field, values: Iterable, ignore_errors: bool = False, inplace: bool = False) -> Native:
        name = get_name(name)
        item_type = self.get_item_type()
        items = map(lambda i, v: set_to_item(name, v, i, item_type=item_type), self.get_items(), values)
        struct = self.get_struct().add_fields(name, inplace=False)
        stream = self.set_items(items, inplace=inplace).set_struct(struct, inplace=inplace)
        if self.is_in_memory():
            if not ignore_errors:
                if not isinstance(values, ARRAY_TYPES):
                    values = list(values)
                if self.get_count() != len(values):
                    msg = 'for add_column() stream and values must have same items count, got {a} != {e}'
                    raise AssertionError(msg.format(e=self.get_count(), a=len(values)))
            stream = stream.to_memory()
        return stream

    def get_str_description(self) -> str:
        rows_count = self.get_str_count()
        cols_count = self.get_column_count()
        cols_str = ', '.join([str(c) for c in self.get_columns()])
        return f'{rows_count} rows, {cols_count} columns: {cols_str}'

    def is_valid_item_type(self, item: Item) -> bool:
        item_type = self.get_item_type()
        if item_type not in (ItemType.Auto, None):
            return item_type.isinstance(item, default=True)
        else:
            return True

    def _is_valid_item(self, item: Item, struct: Struct = None) -> bool:
        if self.is_valid_item_type(item):
            if struct is None:
                struct = self.get_struct()
            if isinstance(struct, StructInterface) or hasattr(struct, 'get_validation_errors'):
                errors = struct.get_validation_errors(item)
                return not errors
            else:
                return True
        else:
            return False

    def _get_validation_errors(self, item: Optional[Item] = None, struct: Struct = None) -> list:
        if item is None:
            item = self.get_one_item()
        if self.is_valid_item_type(item):
            if struct is None:
                struct = self.get_struct()
            if isinstance(struct, StructInterface) or hasattr(struct, 'get_validation_errors'):
                return struct.get_validation_errors(item)
            else:
                return list()
        else:
            expected = self.get_item_type().get_subclasses(skip_missing=True)
            item_type = self.get_item_type()
            msg = f'type({item} is not {expected} for {item_type}'
            return [msg]

    def _get_typing_validated_items(
            self,
            items: Iterable,
            skip_errors: bool = False,
            context: Context = None,
    ) -> Generator:
        for i in items:
            if self.is_valid_item_type(i):
                yield i
            else:
                item_type = self.get_item_type()
                message = f'_get_typing_validated_items() found invalid item {i} for {item_type}'
                if skip_errors:
                    if context:
                        context.get_logger().log(msg=message, level=LoggingLevel.Warning)
                else:
                    raise TypeError(message)

    def _get_validated_items(
            self,
            items: Iterable,
            struct: Struct = None,
            skip_errors: bool = False,
            context: Context = None,  # used for validate items before initialization
    ) -> Generator:
        logger = context.get_logger() if context else self.get_logger()
        for i in items:
            errors = self._get_validation_errors(i, struct=struct)
            if errors:
                method_name = f'{self.__class__.__name__}._get_validated_items()'
                message = f'{method_name} found invalid item {i} for {repr(self)} with errors: {errors}'
                if skip_errors:
                    if logger:
                        logger.log(msg=message, level=LoggingLevel.Warning)
                else:
                    raise TypeError(message)
            else:
                yield i

    def _get_target_item_type(self, *columns, **expressions) -> ItemType:
        input_item_type = self.get_item_type()
        if input_item_type in (ItemType.Any, ItemType.Auto, None):
            if columns and not expressions:
                target_item_type = ItemType.Row
            elif expressions and not columns:
                target_item_type = ItemType.Record
            else:
                target_item_type = ItemType.Auto
        else:
            target_item_type = input_item_type
        return target_item_type

    def _get_enumerated_items(
            self,
            field: Field = SHARP,  # '#'
            first: int = 1,
            item_type: ItemType = ItemType.Auto,
            inplace: bool = False,
    ) -> Iterator[Item]:
        field_name = get_name(field)
        if item_type in (ItemType.Auto, None):
            item_type = self.get_item_type()
        for n, i in super()._get_enumerated_items(item_type=item_type):
            yield set_to_item(field_name, n + first, i, item_type=item_type, inplace=inplace)

    def enumerate(self, field: str = SHARP, first: int = 0, native: bool = True) -> Stream:
        if native:
            return self.stream(
                self._get_enumerated_items(field=field, first=first),
            )
        else:
            return self.stream(
                self._get_enumerated_items(item_type=ItemType.Row, first=first),
                item_type=ItemType.Row,  # KeyValueStream
                secondary=self.get_item_type(),
            )

    def take(self, count: Union[int, bool] = 1, inplace: bool = False) -> Native:
        stream = super().take(count, inplace=inplace) or self
        stream.set_struct(self.get_struct(), check=False, inplace=True)
        return stream

    def skip(self, count: int = 1, inplace: bool = False) -> Native:
        stream = super().skip(count, inplace=inplace)
        struct = self.get_struct()
        if struct is not None and (isinstance(stream, RegularStreamInterface) or hasattr(stream, 'set_struct')):
            stream.set_struct(struct, check=False, inplace=True)
        return stream

    def filter(self, *fields, skip_errors: bool = True, inplace: bool = False, **expressions) -> Native:
        item_type = self.get_item_type()
        filter_function = get_filter_function(*fields, **expressions, item_type=item_type, skip_errors=skip_errors)
        stream = super().filter(filter_function, inplace=inplace)
        struct = self.get_struct()
        if struct is not None and (isinstance(stream, RegularStreamInterface) or hasattr(stream, 'set_struct')):
            stream.set_struct(struct, check=False, inplace=True)
        return self._assume_native(stream)

    def select(self, *columns, use_extended_method: Optional[bool] = None, **expressions) -> Native:
        if use_extended_method is None:
            use_extended_method = self.get_item_type() == ItemType.Row
        input_item_type = self.get_item_type()
        target_item_type = self._get_target_item_type(*columns, **expressions)
        target_struct = sn.get_output_struct(*columns, **expressions, skip_missing=True)
        select_function = sn.get_selection_function(
            *columns, **expressions,
            input_item_type=input_item_type, target_item_type=target_item_type,
            logger=self.get_logger(), selection_logger=self.get_selection_logger(),
            use_extended_method=use_extended_method,
        )
        stream = self.map_to_type(function=select_function, item_type=target_item_type, struct=target_struct)
        return self._assume_native(stream)

    def flat_map(self, function: Callable, to: ItemType = ItemType.Auto) -> Stream:
        items = self._get_mapped_items(function=function, flat=True)
        return self.stream(items, item_type=to, save_count=False)

    def _get_stream_class_by_type(self, item_type: ItemType) -> Class:
        if item_type not in (ItemType.Auto, None):
            stream_type = ItemType.Auto
            if isinstance(item_type, str):
                try:
                    stream_type = StreamType(item_type)
                    item_type = stream_type.get_item_type()
                except ValueError:  # stream_type is not a valid StreamType
                    item_type = ItemType(stream_type)
            if stream_type in (ItemType.Auto, None):
                item_type_name = item_type.get_name()
                stream_type_name = f'{item_type_name}Stream'
                stream_type = StreamType(stream_type_name)
            else:
                stream_type = item_type
            if isinstance(stream_type, StreamType) or hasattr(stream_type, 'get_stream_class'):
                return stream_type.get_stream_class()
            elif isclass(stream_type):
                return stream_type
            else:
                msg = get_type_err_msg(expected=(ItemType, StreamType), got=stream_type, arg='stream_type', caller=2)
                raise TypeError(msg)
        else:
            return self.__class__

    def has_data(self) -> bool:
        count = self.get_estimated_count()
        if count:
            return True
        else:
            return super().has_data()

    def apply_to_data(
            self,
            function: Callable,
            *args,
            dynamic: bool = True,
            item_type: ItemType = ItemType.Auto,
            **kwargs
    ) -> Stream:
        return self.stream(
            self._get_calc(function, *args, **kwargs),
            item_type=item_type,
            ex=self._get_dynamic_meta_fields() if dynamic else None,
        )

    # @deprecated_with_alternative('item_type.get_key_function()')
    def _get_key_function(self, functions: Array, take_hash: bool = False) -> Callable:
        if not isinstance(functions, ARRAY_TYPES):
            functions = [functions]
        item_type = self.get_item_type()
        return item_type.get_key_function(*functions, struct=self.get_struct(), take_hash=take_hash)

    def get_one_column_values(self, column: Field, as_list: bool = False) -> Iterable:
        column_getter = self.get_item_type().get_key_function(column, struct=self.get_struct(), take_hash=False)
        values = map(column_getter, self.get_items())
        if as_list:
            return list(values)
        else:
            return values

    def sort(self, *keys, reverse: bool = False, step: Count = None, verbose: bool = True) -> Native:
        if step is None:
            step = self.get_limit_items_in_memory()
        if keys:
            key_function = self._get_key_function(keys, take_hash=False)
        else:
            key_function = fs.same()
        if self.can_be_in_memory(step=step):
            stream = self.memory_sort(key_function, reverse=reverse, verbose=verbose)
        else:
            stream = self.disk_sort(key_function, reverse=reverse, step=step, verbose=verbose)
        self._assume_native(stream).set_struct(self.get_struct(), check=False, inplace=True)
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
            force_map_side: bool = True,
            merge_function: Optional[Callable] = None,
            verbose: Optional[bool] = None,
    ) -> Native:
        item_type = self.get_item_type()
        if merge_function is None:
            merge_function = fs.merge_two_items(item_type=item_type)
        stream = super(RegularStream, self).join(
            right, key=key, how=how,
            reverse=reverse, is_sorted=is_sorted, right_is_uniq=right_is_uniq,
            allow_map_side=allow_map_side, force_map_side=force_map_side,
            merge_function=merge_function,
            verbose=verbose,
        )
        assert isinstance(stream, RegularStream) or hasattr(stream, 'get_item_type')
        return stream.set_item_type(item_type)

    def _get_grouped_struct(self, *keys, values: Optional[Sequence] = None) -> StructInterface:
        input_struct = self.get_struct()
        output_struct = FlatStruct([])
        key_names = get_names(keys, or_callable=False)
        if values is None:
            values = list()
        elif values is None and input_struct is not None:
            values = list()
            for f in input_struct.get_field_names():
                if f not in key_names:
                    values.append(f)
        for f in list(keys) + list(values):
            if isinstance(f, ARRAY_TYPES):
                field_name = get_name(f[0])
            elif isinstance(f, FieldNo):
                if input_struct is not None:
                    field_name = input_struct.get_field_description(f)
                else:
                    field_name = f'column{f:02}'
            else:
                field_name = get_name(f)
            if f in values:
                value_type = ValueType.Sequence
            elif isinstance(f, FieldInterface) or hasattr(f, 'get_value_type'):
                value_type = f.get_value_type()
            elif input_struct:
                value_type = input_struct.get_field_description(f).get_value_type() or ValueType.Any
            else:
                value_type = ValueType.Any
            output_struct.append_field(field_name, value_type)
        return output_struct

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
            values: Columns = None,
            skip_missing: bool = False,
            as_pairs: bool = False,  # deprecated argument
            output_struct: Struct = None,
            take_hash: bool = False,
    ) -> Stream:
        keys = unfold_structs_to_fields(keys)
        key_function = self._get_key_function(keys, take_hash=take_hash)
        iter_groups = self._get_groups(key_function, as_pairs=as_pairs)
        count = self.get_count() or self.get_estimated_count()
        if count == 0 and not skip_missing:
            raise AssertionError('Got empty stream.')
        if output_struct is not None:
            expected_struct = output_struct
        elif as_pairs:
            expected_struct = FlatStruct(['key', 'value']).set_types(key=ValueType.Any, value=ValueType.Any)
        else:
            expected_struct = self._get_grouped_struct(*keys, values=values)
        if as_pairs:
            stream_class = StreamType.KeyValueStream.get_class()
            stream_groups = stream_class(iter_groups, value_item_type=self.get_item_type())
        else:
            stream_groups = self.stream(
                iter_groups,
                item_type=ItemType.Row,
                struct=expected_struct,
                check=False,
            )
        if values:
            item_type = self.get_item_type()
            if item_type == ItemType.Any:
                raise TypeError('For untyped items (ItemType.Any) values-option of sorted_group_by() not supported')
            elif item_type == ItemType.Row:
                input_struct = self.get_struct()
                keys = [item_type.get_field_getter(f, struct=input_struct) for f in keys]
                values = [item_type.get_field_getter(f, struct=input_struct) for f in values]
            fold_mapper = fs.fold_lists(
                keys=keys, values=values,
                as_pairs=as_pairs, skip_missing=skip_missing,
                item_type=item_type,
            )
            stream_groups = stream_groups.map_to_type(fold_mapper, item_type=item_type)
            if expected_struct is not None:
                stream_groups.set_struct(expected_struct, check=False, inplace=True)
        if self.is_in_memory():
            return stream_groups.to_memory()
        else:
            stream_groups.set_estimated_count(count, inplace=True)
            return stream_groups

    def group_by(
            self,
            *keys,
            values: Columns = None,
            as_pairs: bool = False,  # deprecated argument, use group_to_pairs() instead
            take_hash: bool = True,
            step: Count = None,
            skip_missing: bool = False,
            verbose: bool = True,
    ) -> Stream:
        keys = unfold_structs_to_fields(keys)
        if as_pairs:
            key_for_sort = keys
        else:
            key_for_sort = self._get_key_function(keys, take_hash=take_hash)
        return self.sort(
            key_for_sort,
            step=step,
            verbose=verbose,
        ).sorted_group_by(
            *keys,
            values=values,
            as_pairs=as_pairs,
            skip_missing=skip_missing,
        )

    @deprecated_with_alternative('RegularStream.group_by(as_pairs=True)')
    def group_to_pairs(
            self,
            *keys,
            values: Columns = None,
            step: Count = None,
            verbose: bool = True,
    ) -> RegularStreamInterface:
        grouped_stream = self.group_by(*keys, values=values, step=step, as_pairs=True, take_hash=False, verbose=verbose)
        return self._assume_native(grouped_stream)

    def uniq(self, *keys, sort: bool = False) -> Native:
        if sort:
            stream = self.sort(*keys)
        else:
            stream = self
        items = stream._get_uniq_items(*keys)
        result = self.stream(items, count=None)
        return self._assume_native(result)

    def _get_uniq_items(self, *keys) -> Iterable:
        keys = unfold_structs_to_fields(keys)
        key_fields = get_names(keys, or_callable=True)
        key_function = self._get_key_function(key_fields, take_hash=False)
        prev_value = None
        is_first = True
        for i in self.get_items():
            value = key_function(i)
            if is_first or value != prev_value:
                yield i
                is_first = False
            prev_value = value

    def get_dict(
            self,
            key: UniKey = fs.first(),
            value: UniKey = fs.second(),
            of_lists: bool = False,
            skip_errors: bool = False,
    ) -> dict:
        key_func = self._get_key_function(key)
        value_func = self._get_key_function(value)
        if of_lists:
            result = dict()
            for k, v in self._get_mapped_items(lambda i: (key_func(i), value_func(i)), skip_errors=skip_errors):
                if k in result:
                    result[k].append(v)
                else:
                    result[k] = [v]
            return result
        else:
            return super(RegularStream, self).get_dict(key=key_func, value=value_func)

    def get_validation_message(self, skip_disconnected: bool = True) -> str:
        validation_errors = self._get_validation_errors()
        if validation_errors:
            errors_str = ', '.join(validation_errors)
            return f'[INVALID] Validation errors: {errors_str}'
        else:
            columns_count = self.get_column_count()
            return f'Stream has {columns_count} valid columns:'

    def is_existing(self) -> bool:  # used in ValidateMixin.prepare_examples_with_title()
        return True

    def is_actual(self) -> bool:  # used in ValidateMixin.prepare_examples_with_title()
        return True

    def actualize(self) -> Native:  # used in ValidateMixin.prepare_examples_with_title()
        return self

    @staticmethod
    def _assume_stream(stream) -> RegularStreamInterface:
        return stream

    @staticmethod
    def _assume_native(stream) -> Native:
        return stream

    def to_stream(
            self,
            data: Optional[Iterable] = None,
            item_type: ItemType = ItemType.Auto,
            ex: OptionalFields = None,
            **kwargs
    ) -> Stream:
        if item_type in (ItemType.Auto, None):
            item_type = self.get_item_type()
        elif isinstance(item_type, StreamType):
            item_type = item_type.get_item_type()
        if data is None:
            if hasattr(self, 'get_items_of_type'):
                data = self.get_items_of_type(item_type)
            else:
                data = self.get_data()
        meta = self.get_meta(ex=ex)
        meta.update(kwargs)
        if 'count' not in meta:
            meta['count'] = self.get_count()
        if 'source' not in meta:
            meta['source'] = self.get_source()
        stream = StreamBuilder.stream(data, item_type=item_type, **meta)
        return self._assume_stream(stream)

    def to_file(
            self,
            file: Union[FileObj, FileName],
            verbose: bool = True,
            return_stream: bool = True,
            **kwargs
    ) -> Native:
        if isinstance(file, FileName):
            file = self.get_context().get_job_folder().file(file, **kwargs)
        if not (isinstance(file, FileObj) or hasattr(file, 'write_stream')):
            msg = get_type_err_msg(expected=(FileObj, FileName), got=file, arg='file', caller=RegularStream.to_file)
            raise TypeError(msg)
        meta = self.get_meta()
        file.write_stream(self, verbose=verbose)
        if return_stream:
            return file.to_stream_type(item_type=self.get_item_type(), verbose=verbose).update_meta(**meta)

    @classmethod
    @deprecated_with_alternative('connectors.filesystem.local_file.JsonFile.to_stream()')
    def from_json_file(
            cls,
            filename: str,
            item_type: ItemType = ItemType.Record,
            skip_first_line: bool = False,
            max_count: Count = None,
            check: Optional[bool] = None,
            verbose: bool = False,
    ) -> Stream:
        line_stream_class = StreamType.LineStream.get_class()
        return line_stream_class.from_text_file(
            filename,
            skip_first_line=skip_first_line, max_count=max_count,
            check=check, verbose=verbose,
        ).to_stream(item_type=item_type)

    def __getitem__(self, item):
        assert self.is_in_memory()
        data = self.get_stream_data()
        assert isinstance(data, Sequence), f'got data={data}'
        return data[item]


StreamBuilder.set_default_stream_class(RegularStream)
