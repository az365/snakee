from typing import Optional, Callable, Iterable, Generator, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        LoggerInterface, StreamInterface, FieldInterface, StructInterface, StructRowInterface,
        LoggingLevel, StreamType, ValueType, ItemType, JoinType, How,
        Field, Name, FieldName, FieldNo, UniKey, Item, Row, ROW_SUBCLASSES,
        AUTO, Auto, AutoBool, Source, Context, TmpFiles, Count, Columns, AutoColumns, Array, ARRAY_TYPES,
    )
    from base.functions.arguments import get_name, get_names
    from utils.decorators import deprecated_with_alternative
    from utils.external import pd, DataFrame, get_use_objects_for_output
    from loggers.fallback_logger import FallbackLogger
    from functions.secondary import all_secondary_functions as fs
    from content.items.item_getters import value_from_struct_row
    from content.selection import selection_classes as sn
    from content.struct.flat_struct import FlatStruct
    from content.struct.struct_mixin import StructMixin
    from content.struct.struct_row import StructRow
    from streams.mixin.convert_mixin import ConvertMixin
    from streams.regular.row_stream import RowStream
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        LoggerInterface, StreamInterface, FieldInterface, StructInterface, StructRowInterface,
        LoggingLevel, StreamType, ValueType, ItemType, JoinType, How,
        Field, Name, FieldName, FieldNo, UniKey, Item, Row, ROW_SUBCLASSES,
        AUTO, Auto, AutoBool, Source, Context, TmpFiles, Count, Columns, AutoColumns, Array, ARRAY_TYPES,
    )
    from ...base.functions.arguments import get_name, get_names
    from ...utils.decorators import deprecated_with_alternative
    from ...utils.external import pd, DataFrame, get_use_objects_for_output
    from ...loggers.fallback_logger import FallbackLogger
    from ...functions.secondary import all_secondary_functions as fs
    from ...content.items.item_getters import value_from_struct_row
    from ...content.selection import selection_classes as sn
    from ...content.struct.flat_struct import FlatStruct
    from ...content.struct.struct_mixin import StructMixin
    from ...content.struct.struct_row import StructRow
    from ..mixin.convert_mixin import ConvertMixin
    from .row_stream import RowStream

Native = Union[StreamInterface, StructRowInterface]
Struct = StructInterface
OptStruct = Union[Struct, Iterable, None]

DYNAMIC_META_FIELDS = ('count', 'struct')
NAME_POS, TYPE_POS, HINT_POS = 0, 1, 2  # old style struct fields
DICT_CAST_TYPES = dict(bool=bool, int=int, float=float, str=str, text=str, date=str)
DEFAULT_EXAMPLE_COUNT = 10


def is_row(row: Row) -> bool:
    return isinstance(row, StructRow) or isinstance(row, ROW_SUBCLASSES)


def get_validation_errors(row: Row, struct: OptStruct) -> list:
    if is_row(row):
        if isinstance(row, StructRow):
            row = row.get_data()
        if isinstance(struct, FlatStruct):
            return struct.get_validation_errors(row)
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
        raise TypeError(f'Expected struct as Struct or Iterable, got {struct}')


class StructStream(RowStream, StructMixin, ConvertMixin):
    def __init__(
            self,
            data: Iterable,
            struct: OptStruct = None,
            name: Union[Name, Auto] = AUTO,
            caption: str = '',
            count: Count = None,
            less_than: Count = None,
            source: Source = None,
            context: Context = None,
            max_items_in_memory: Count = AUTO,
            tmp_files: TmpFiles = AUTO,
            check: bool = True,
    ):
        if check:
            data = self._get_validated_items(data, struct=struct)
        super().__init__(
            data=data, check=False,
            struct=struct,
            name=name, caption=caption,
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
        elif isinstance(field, sn.AbstractDescription) or hasattr(field, 'get_function'):
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

    # @deprecated_with_alternative('item_type.get_key_function()')
    def _get_key_function(self, descriptions: Array, take_hash: bool = False) -> Callable:
        return self.get_item_type().get_key_function(*descriptions, struct=self.get_struct(), take_hash=take_hash)

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

    def get_struct_rows(
            self,
            rows: Iterable,
            struct: Union[Struct, Auto] = AUTO,
            skip_bad_rows: bool = False,
            skip_bad_values: bool = False,
            verbose: bool = True,
    ) -> Generator:
        struct = Auto.delayed_acquire(struct, self.get_struct)
        if isinstance(struct, StructInterface) or hasattr(struct, 'get_converters'):  # actual approach
            converters = struct.get_converters(src='str', dst='py')
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
                    raise ValueError(err_msg.format(item_type))
            elif isinstance(i, ROW_SUBCLASSES):
                if item_type == ItemType.Row:
                    yield i
                elif item_type == ItemType.StructRow:
                    yield StructRow(i, self.get_struct())
                elif item_type == ItemType.Record:
                    yield {k: v for k, v in zip(columns, i)}
                else:
                    raise ValueError(err_msg.format(item_type))
            else:
                msg = 'StructStream.get_items_of_type(item_type={}): Expected items as Row or StructRow, got {} as {}'
                raise TypeError(msg.format(item_type, i, type(i)))

    def skip(self, count: int = 1, inplace: bool = False) -> Native:
        return super().skip(count, inplace=inplace).update_meta(struct=self.get_struct())

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
            self,
            count: Count = DEFAULT_EXAMPLE_COUNT,
            filters: Columns = None,
            columns: Columns = None,
    ) -> Optional[list]:
        sm_sample = self.filter(*filters) if filters else self
        sm_sample = sm_sample.to_record_stream()
        return sm_sample.get_list()

    @staticmethod
    def _assume_native(obj) -> Native:
        return obj
