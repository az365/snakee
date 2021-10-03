from typing import Optional, Union, Iterable, Callable

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg, selection as sf
    from interfaces import (
        StreamInterface, StructInterface, StructRowInterface, Row, Item, ItemType,
        AUTO, Auto, Source, Context, TmpFiles, Count,
    )
    from streams import stream_classes as sm
    from loggers import logger_classes as log
    from functions import all_functions as fs
    from selection import selection_classes as sn
    from items.flat_struct import FlatStruct
    from items.struct_row import StructRow
    from items.legacy_classes import get_validation_errors as get_legacy_validation_errors
    from utils.decorators import deprecated_with_alternative
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg, selection as sf
    from ...interfaces import (
        StreamInterface, StructInterface, StructRowInterface, Row, Item, ItemType,
        AUTO, Auto, Source, Context, TmpFiles, Count,
    )
    from .. import stream_classes as sm
    from ...loggers import logger_classes as log
    from ...functions import all_functions as fs
    from ...selection import selection_classes as sn
    from ...items.flat_struct import FlatStruct
    from ...items.struct_row import StructRow
    from ...items.legacy_classes import get_validation_errors as get_legacy_validation_errors
    from ...utils.decorators import deprecated_with_alternative

Native = Union[StreamInterface, StructRowInterface]
Struct = StructInterface
OptStruct = Optional[Union[Struct, Iterable]]

DYNAMIC_META_FIELDS = ('count', 'struct')
NAME_POS, TYPE_POS, HINT_POS = 0, 1, 2  # old style struct fields


def is_row(row: Row) -> bool:
    return isinstance(row, (list, tuple))


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


def get_validation_errors(row: Row, struct: OptStruct) -> list:
    if is_row(row):
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
                    logger.log(msg=message, level=log.LoggingLevel.Error.value)
                if skip_bad_values:
                    if logger:
                        message = 'Skipping bad value in row:'.format(list(zip(row, struct)))
                        logger.log(msg=message, level=log.LoggingLevel.Debug.value)
                    new_value = None
                else:
                    message = 'Error in row: {}...'.format(str(list(zip(row, struct)))[:80])
                    if logger:
                        logger.log(msg=message, level=log.LoggingLevel.Warning.value)
                    else:
                        log.get_logger().show(message)
                    raise e
            row[c] = new_value
        return row
    else:
        raise TypeError


class StructStream(sm.RowStream):
    def __init__(
            self,
            data, struct: OptStruct = None,
            name=arg.DEFAULT, check=True,
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

    @classmethod
    def is_valid_item_type(cls, item: Item) -> bool:
        return super().is_valid_item_type(item)

    def _is_valid_item(self, item) -> bool:
        return is_valid(item, struct=self.get_struct())

    def _get_validated_items(self, items, struct=arg.DEFAULT, skip_errors=False, context=arg.NOT_USED):
        if struct == arg.DEFAULT:
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

    def get_struct_rows(self, rows, struct=arg.AUTO, skip_bad_rows=False, skip_bad_values=False, verbose=True):
        struct = arg.undefault(struct, self.get_struct())
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
            *args, **kwargs,
            target_item_type=self.get_item_type(), input_item_type=self.get_item_type(),
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
        selection_method = sf.value_from_struct_row

        def filter_function(r):
            for f in expressions_list:
                if not selection_method(r, f):
                    return False
            return True
        result = self.stream(
            filter(filter_function, self.get_items()),
        )
        return result.to_memory() if self.is_in_memory() else result

    @staticmethod
    def _assume_native(obj) -> Native:
        return obj
