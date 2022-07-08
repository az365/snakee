from typing import Callable, Iterable, Generator, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        LoggerInterface, StreamInterface, FieldInterface, StructInterface, StructRowInterface, Struct,
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
        LoggerInterface, StreamInterface, FieldInterface, StructInterface, StructRowInterface, Struct,
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

DYNAMIC_META_FIELDS = 'count', 'struct'


class StructStream(RowStream, StructMixin, ConvertMixin):
    def __init__(
            self,
            data: Iterable,
            struct: Struct = None,
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
        super().__init__(
            data=data, struct=struct, check=check,
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

    def get_struct_rows(
            self,
            rows: Union[Iterable, Auto],
            struct: Union[Struct, Auto] = AUTO,
            skip_bad_rows: bool = False,
            skip_bad_values: bool = False,
            skip_missing: bool = False,
    ) -> Generator:
        rows = Auto.delayed_acquire(rows, self.get_rows)
        struct = Auto.delayed_acquire(struct, self.get_struct)
        if isinstance(struct, StructInterface) or hasattr(struct, 'get_converters'):  # actual approach
            converters = struct.get_converters(src='str', dst='py')
            for r in rows:
                converted_row = list()
                for value, converter in zip(r, converters):
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
                    converted_row.append(converted_value)
                if converted_row is not None:
                    yield converted_row.copy()
        elif skip_missing:
            yield from rows
        else:
            raise TypeError(f'StructStream.get_struct_rows(): Expected struct as StructInterface, got {struct}')

    def structure(self, struct: Struct, skip_bad_rows: bool = False, skip_bad_values: bool = False, verbose=True):
        struct_rows = self.get_struct_rows(AUTO, skip_bad_rows=skip_bad_rows, skip_bad_values=skip_bad_values)
        count = None if skip_bad_rows else self.get_count(),
        return self.stream(struct_rows, struct=struct, count=count, check=False)

    @staticmethod
    def _assume_native(obj) -> Native:
        return obj
