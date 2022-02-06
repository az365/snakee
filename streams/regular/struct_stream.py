from typing import Optional, Callable, Iterable, Generator, Union

try:  # Assume we're a submodule in a package.
    from base.classes.auto import Auto, AUTO
    from base.functions.arguments import get_name, get_names
    from utils import selection as sf
    from utils.decorators import deprecated_with_alternative
    from utils.external import pd, DataFrame, get_use_objects_for_output
    from interfaces import (
        LoggerInterface, StreamInterface, FieldInterface, StructInterface, StructRowInterface,
        LoggingLevel, StreamType, FieldType, ItemType, JoinType, How,
        Field, Name, FieldName, FieldNo, UniKey, Item, Row, ROW_SUBCLASSES,
        AUTO, Auto, AutoBool, Source, Context, TmpFiles, Count, Columns, AutoColumns, Array, ARRAY_TYPES,
    )
    from loggers.fallback_logger import FallbackLogger
    from streams import stream_classes as sm
    from functions.secondary import all_secondary_functions as fs
    from content.selection import selection_classes as sn
    from content.struct.flat_struct import FlatStruct
    from content.struct.struct_mixin import StructMixin
    from content.struct.struct_row import StructRow
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.auto import Auto, AUTO
    from ...base.functions.arguments import get_name, get_names
    from ...utils import selection as sf
    from ...utils.decorators import deprecated_with_alternative
    from ...utils.external import pd, DataFrame, get_use_objects_for_output
    from ...interfaces import (
        LoggerInterface, StreamInterface, FieldInterface, StructInterface, StructRowInterface,
        LoggingLevel, StreamType, FieldType, ItemType, JoinType, How,
        Field, Name, FieldName, FieldNo, UniKey, Item, Row, ROW_SUBCLASSES,
        AUTO, Auto, AutoBool, Source, Context, TmpFiles, Count, Columns, AutoColumns, Array, ARRAY_TYPES,
    )
    from ...loggers.fallback_logger import FallbackLogger
    from .. import stream_classes as sm
    from ...functions.secondary import all_secondary_functions as fs
    from ...content.selection import selection_classes as sn
    from ...content.struct.flat_struct import FlatStruct
    from ...content.struct.struct_mixin import StructMixin
    from ...content.struct.struct_row import StructRow

Native = Union[StreamInterface, StructRowInterface]
Struct = StructInterface
OptStruct = Union[Struct, Iterable, None]

DYNAMIC_META_FIELDS = ('count', 'struct')
NAME_POS, TYPE_POS, HINT_POS = 0, 1, 2  # old style struct fields
DICT_CAST_TYPES = dict(bool=bool, int=int, float=float, str=str, text=str, date=str)
DEFAULT_EXAMPLE_COUNT = 10


def is_row(row: Row) -> bool:
    return isinstance(row, StructRow) or isinstance(row, ROW_SUBCLASSES)


# deprecated
def is_valid(row: Row, struct: OptStruct) -> bool:
    if is_row(row):
        if isinstance(struct, Struct):
            return struct.is_valid_row(row)
        elif isinstance(struct, Iterable):
            types = list()
            default_type = str
            for description in struct:
                field_type = description[TYPE_POS]
                if field_type in fs.DICT_CAST_TYPES.values():
                    types.append(field_type)
                else:
                    types.append(fs.DICT_CAST_TYPES.get(field_type, default_type))
            for value, field_type in zip(row, types):
                if not isinstance(value, field_type):
                    return False
            return True
        elif struct is None:
            return True


# deprecated
def get_legacy_validation_errors(row: Iterable, struct: Union[StructInterface, Iterable], default_type=str):
    if isinstance(struct, StructInterface) or hasattr(struct, 'get_fields_descriptions'):
        iter_struct = struct.get_fields_descriptions()
    else:
        assert isinstance(struct, Iterable)
        iter_struct = struct
    validation_errors = list()
    names = list()
    types = list()
    for description in iter_struct:
        field_name = description[NAME_POS]
        field_type = description[TYPE_POS]
        names.append(field_name)
        if field_type not in DICT_CAST_TYPES.values():
            field_type = DICT_CAST_TYPES.get(field_type, default_type)
        types.append(field_type)
    for value, field_name, field_type in zip(row, names, types):
        if not isinstance(value, field_type):
            template = 'Field {}: type {} expected, got {} (value={})'
            msg = template.format(field_name, field_type, type(value), value)
            validation_errors.append(msg)
    return validation_errors


def get_validation_errors(row: Row, struct: OptStruct) -> list:
    if is_row(row):
        if isinstance(row, StructRow):
            row = row.get_data()
        if isinstance(struct, FlatStruct):
            return struct.get_validation_errors(row)
        elif isinstance(struct, Iterable):
            return get_legacy_validation_errors(row, struct)
    else:
        msg = 'Expected row as list or tuple, got {} (row={})'.format(type(row), row)
        return [msg]


def check_rows(rows, struct: OptStruct, skip_errors: bool = False) -> Iterable:
    for r in rows:
        validation_errors = get_validation_errors(r, struct=struct)
        if not validation_errors:
            pass
        elif skip_errors:
            continue
        else:
            raise TypeError('check_rows() got validation errors: {}'.format(validation_errors))
        yield r


@deprecated_with_alternative('struct.StructRow()')
def apply_struct_to_row(row, struct: OptStruct, skip_bad_values=False, logger=None):
    if isinstance(struct, Struct) or hasattr(struct, 'get_converters'):
        converters = struct.get_converters('str', 'py')
        return [converter(value) for value, converter in zip(row, converters)]
    elif isinstance(struct, Iterable):
        for c, (value, description) in enumerate(zip(row, struct)):
            field_type = description[TYPE_POS]
            try:
                cast_function = fs.cast(field_type)
                new_value = cast_function(value)
            except ValueError as e:
                field_name = description[NAME_POS]
                if logger:
                    message = 'Error while casting field {} ({}) with value {} into type {}'.format(
                        field_name, c,
                        value, field_type,
                    )
                    logger.log(msg=message, level=LoggingLevel.Error)
                if skip_bad_values:
                    if logger:
                        message = 'Skipping bad value in row:'.format(list(zip(row, struct)))
                        logger.log(msg=message, level=LoggingLevel.Debug)
                    new_value = None
                else:
                    message = 'Error in row: {}...'.format(str(list(zip(row, struct)))[:80])
                    if logger:
                        logger.log(msg=message, level=LoggingLevel.Warning)
                    else:
                        FallbackLogger().log(message)
                    raise e
            row[c] = new_value
        return row
    else:
        raise TypeError


class StructStream(sm.RowStream, StructMixin, sm.ConvertMixin):
    def __init__(
            self,
            data: Iterable,
            struct: OptStruct = None,
            name: Union[Name, Auto] = AUTO, check: bool = True,
            count: Count = None, less_than: Count = None,
            source: Source = None, context: Context = None,
            max_items_in_memory: Count = AUTO,
            tmp_files: TmpFiles = AUTO,
    ):
        self._struct = struct or list()
        if check:
            data = self._get_validated_items(data, struct=struct)
        super().__init__(
            data=data,
            name=name, check=False,
            count=count, less_than=less_than,
            source=source, context=context,
            max_items_in_memory=max_items_in_memory,
            tmp_files=tmp_files,
        )

    @staticmethod
    def _get_dynamic_meta_fields() -> tuple:
        return DYNAMIC_META_FIELDS

    @staticmethod
    def get_item_type() -> ItemType:
        return ItemType.StructRow

    def get_field_getter(self, field: Field) -> Callable:
        if isinstance(field, Callable):
            func = field
        elif isinstance(field, sn.AbstractDescription) or hasattr(field, 'get_functions'):
            func = field.get_function()
        else:  # isinstance(field, Field)
            if isinstance(field, FieldNo):  # int
                field_no = field
            else:  # isinstance(field, (FieldName, FieldInterface))
                if isinstance(field, FieldName):  # str
                    field_name = field
                else:  # isinstance(field, FieldInterface)
                    field_name = get_name(field)
                field_no = self.get_field_position(field_name)
            func = fs.partial(lambda r, n: r[n], field_no)
        return func

    def _get_key_function(self, descriptions: Array, take_hash: bool = False) -> Callable:
        descriptions = get_names(descriptions, or_callable=True)
        if len(descriptions) == 0:
            raise ValueError('key must be defined')
        elif len(descriptions) == 1:
            desc = descriptions[0]
            key_function = self.get_field_getter(desc)
        else:
            if isinstance(descriptions[0], Callable):
                func = descriptions[0]
                fields = descriptions[1:]
            elif isinstance(descriptions[-1], Callable):
                func = descriptions[-1]
                fields = descriptions[:-1]
            else:
                func = tuple
                fields = descriptions
            arg_getters = [self.get_field_getter(f) for f in fields]
            key_function = lambda r: func([f(r) for f in arg_getters])
        if take_hash:
            return lambda r: hash(key_function(r))
        else:
            return key_function

    @classmethod
    def is_valid_item_type(cls, item: Item) -> bool:
        return super().is_valid_item_type(item)

    def _is_valid_item(self, item) -> bool:
        validation_errors = get_validation_errors(item, struct=self.get_struct())
        return not validation_errors

    def _get_validated_items(self, items, struct=AUTO, skip_errors=False, context=None):
        if struct == AUTO:
            struct = self.get_struct()
        return check_rows(items, struct, skip_errors)

    def get_struct(self) -> Struct:
        return self._struct

    def set_struct(self, struct: Struct, check: bool = True, inplace: bool = False):
        if inplace:
            self._struct = struct
        else:
            return self.stream(
                check_rows(self.get_data(), struct=struct) if check else self.get_data(),
                struct=struct,
            )

    def get_struct_rows(self, rows, struct=AUTO, skip_bad_rows=False, skip_bad_values=False, verbose=True):
        struct = Auto.delayed_acquire(struct, self.get_struct)
        if isinstance(struct, StructInterface):  # actual approach
            converters = struct.get_converters('str', 'py')
            for r in rows:
                converted_row = list()
                for value, converter in zip(r, converters):
                    converted_value = converter(value)
                    converted_row.append(converted_value)
                yield converted_row.copy()
        else:  # deprecated approach
            for r in rows:
                if skip_bad_rows:
                    try:
                        yield apply_struct_to_row(r, struct, False, logger=self if verbose else None)
                    except ValueError:
                        self.log(['Skip bad row:', r], verbose=verbose)
                else:
                    yield apply_struct_to_row(r, struct, skip_bad_values, logger=self if verbose else None)

    def structure(self, struct: Struct, skip_bad_rows: bool = False, skip_bad_values: bool = False, verbose=True):
        return self.stream(
            self.get_struct_rows(self.get_items()),
            struct=struct,
            count=None if skip_bad_rows else self.get_count(),
            check=False,
        )

    def get_columns(self) -> list:
        struct = self.get_struct()
        if isinstance(struct, StructInterface):
            return struct.get_columns()
        elif isinstance(struct, Iterable):
            return [c[0] for c in struct]

    def get_items(self, item_type: Union[ItemType, Auto] = AUTO) -> Iterable:
        if Auto.is_defined(item_type):
            return self.get_items_of_type(item_type)
        else:
            return self.get_stream_data()

    def get_items_of_type(self, item_type: ItemType) -> Generator:
        err_msg = 'StructStream.get_items_of_type(item_type): Expected StructRow, Row, Record, got item_type={}'
        columns = list(self.get_columns())
        for i in self.get_stream_data():
            if isinstance(i, StructRow):
                if item_type == ItemType.StructRow:
                    yield i
                elif item_type == ItemType.Row:
                    yield i.get_data()
                elif item_type == ItemType.Record:
                    yield {k: v for k, v in zip(columns, i.get_data())}
                else:
                    raise TypeError(err_msg.format(item_type))
            elif isinstance(i, ROW_SUBCLASSES):
                if item_type == ItemType.Row:
                    yield i
                elif item_type == ItemType.StructRow:
                    yield StructRow(i, self.get_struct())
                elif item_type == ItemType.Record:
                    yield {k: v for k, v in zip(columns, i)}
                else:
                    raise TypeError(err_msg.format(item_type))
            else:
                msg = 'StructStream.get_items_of_type(item_type={}): Expected items as Row or StructRow, got {} as {}'
                raise TypeError(msg.format(item_type, i, type(i)))

    def struct_map(self, function: Callable, struct: Struct):
        return self.__class__(
            map(function, self.get_items()),
            count=self.get_count(),
            less_than=self.get_count() or self.get_estimated_count(),
            struct=struct,
        )

    def skip(self, count: int = 1):
        return super().skip(count).update_meta(struct=self.get_struct())

    def select(self, *args, **kwargs):
        selection_description = sn.SelectionDescription.with_expressions(
            fields=get_names(args, or_callable=True), expressions=kwargs,
            input_item_type=self.get_item_type(), target_item_type=self.get_item_type(),
            input_struct=self.get_struct(),
            logger=self.get_logger(), selection_logger=self.get_selection_logger(),
        )
        selection_function = selection_description.get_mapper()
        output_struct = selection_description.get_output_struct()
        return self.struct_map(
            function=selection_function,
            struct=output_struct,
        )

    def filter(self, *fields, **expressions) -> Native:
        primitives = (str, int, float, bool)
        expressions_list = [(k, fs.equal(v) if isinstance(v, primitives) else v) for k, v in expressions.items()]
        expressions_list = list(fields) + expressions_list
        expressions_list = [sn.translate_names_to_columns(e, struct=self.get_struct()) for e in expressions_list]
        selection_method = sf.value_from_struct_row

        def filter_function(r):
            for f in expressions_list:
                if not selection_method(r, f):
                    return False
            return True
        filtered_items = filter(filter_function, self.get_items())
        result = self.stream(filtered_items)
        return result.to_memory() if self.is_in_memory() else result

    def sorted_group_by(
            self,
            *keys,
            values: Optional[Iterable] = None,
            as_pairs: bool = False,
    ) -> StreamInterface:
        if as_pairs:
            return super().sorted_group_by(*keys, values=values, as_pairs=True)
        else:
            output_struct = FlatStruct([])
            for f in list(keys) + list(values):
                if isinstance(f, ARRAY_TYPES):
                    field_name = get_name(f[0])
                else:
                    field_name = get_name(f)
                if f in values:
                    field_type = FieldType.Tuple
                elif isinstance(f, FieldInterface) or hasattr(f, 'get_type'):
                    field_type = f.get_type()
                else:
                    field_type = AUTO
                output_struct.append_field(field_name, field_type)
            return super().sorted_group_by(*keys, values=values, as_pairs=False, output_struct=output_struct)

    def map_side_join(
            self,
            right: StreamInterface,
            key: UniKey,
            how: How = JoinType.Left,
            right_is_uniq: bool = True,
    ) -> StreamInterface:
        if right.get_stream_type() != StreamType.RecordStream:
            right = right.to_record_stream()
        stream = self.to_record_stream().map_side_join(right, key=key, how=how, right_is_uniq=right_is_uniq)
        return stream

    def _get_field_getter(self, field: UniKey, item_type: Union[ItemType, Auto] = AUTO, default=None):
        field_position = self.get_field_position(field)
        return lambda i: i[field_position]

    def get_rows(self, **kwargs) -> Generator:
        assert not kwargs, 'StructStream.get_rows(**{}): kwargs not supported'.format(kwargs)
        for r in self.get_stream_data():
            if isinstance(r, ROW_SUBCLASSES):
                yield r
            elif isinstance(r, StructRow) or hasattr(r, 'get_data'):
                yield r.get_data()
            else:
                msg = 'StructStream.get_rows(): Expected Row or StructRow, got {} as {}'
                raise TypeError(msg.format(r, type(r)))

    def get_records(self, columns: AutoColumns = AUTO) -> Generator:
        if Auto.is_defined(columns):
            available_columns = self.get_columns()
            for r in self.get_rows():
                yield {k: v for k, v in zip(available_columns, r) if k in columns}
        else:
            yield from self.get_items_of_type(item_type=ItemType.Record)

    def to_record_stream(self, *args, **kwargs) -> StreamInterface:
        assert not args, 'StructStream.to_record_stream(): args not supported, got *{}'.format(args)
        records = self.get_records()
        return self.stream(records, stream_type=StreamType.RecordStream, **kwargs)

    def get_demo_example(
            self, count: Count = DEFAULT_EXAMPLE_COUNT,
            filters: Columns = None, columns: Columns = None,
            as_dataframe: AutoBool = AUTO,
    ) -> Union[DataFrame, list, None]:
        as_dataframe = Auto.delayed_acquire(as_dataframe, get_use_objects_for_output)
        sm_sample = self.filter(*filters) if filters else self
        sm_sample = sm_sample.to_record_stream()
        if as_dataframe and hasattr(sm_sample, 'get_dataframe'):
            return sm_sample.get_dataframe(columns)
        elif hasattr(sm_sample, 'get_list'):
            return sm_sample.get_list()

    @staticmethod
    def _assume_native(obj) -> Native:
        return obj
