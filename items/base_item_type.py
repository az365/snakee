from typing import Callable, Union, Any, NoReturn

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from utils.enum import SubclassesType
    from utils.decorators import deprecated_with_alternative
    from items.struct_row_interface import StructRowInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..utils.enum import SubclassesType
    from ..utils.decorators import deprecated_with_alternative
    from .struct_row_interface import StructRowInterface

Field = Union[int, str, Callable]
Value = Any
Array = Union[list, tuple]
Row = Array
Record = dict
Line = str
SimpleItem = Union[Line, Row, Record]
SelectableItem = Union[Row, Record, StructRowInterface]
Item = Union[SimpleItem, StructRowInterface]

STAR = '*'


class ItemType(SubclassesType):
    Line = 'line'
    Row = 'row'
    Record = 'record'
    SchemaRow = 'schema_row'
    Any = 'any'
    Auto = arg.DEFAULT

    _auto_value = False  # option: do not update auto-value for ItemType.Auto

    @staticmethod
    def _get_selectable_types():
        return ItemType.Record, ItemType.Row, ItemType.SchemaRow

    def is_selectable(self):
        return self in self._get_selectable_types()

    @deprecated_with_alternative('ItemType.get_value_from_item()')
    def get_field_value_from_item(self, field: Field, item: Item, skip_errors: bool = True):
        if isinstance(field, Callable):
            return field(item)
        if skip_errors:
            if self == ItemType.Row or isinstance(field, int) or isinstance(item, (list, tuple)):
                if field < len(item):
                    return item[field]
            elif self == ItemType.Record or isinstance(field, str) or isinstance(item, dict):
                return item.get(field)
        else:
            assert (
                self == ItemType.Row and isinstance(field, int) and isinstance(item, (list, tuple))
            ) or (
                self == ItemType.Record and isinstance(field, str) and isinstance(item, dict)
            )
            return item[field]

    def get_value_from_item(self, item: SelectableItem, field: Field, default=None, skip_unsupported_types=False):
        if self == ItemType.Auto:
            item_type = self.detect(item, default=ItemType.Any)
            assert isinstance(item_type, ItemType)
            return item_type.get_value_from_item(item, field, default)
        elif self == ItemType.Row:
            return get_field_value_from_row(column=field, row=item, default=default, skip_missing=False)
        elif self == ItemType.Record:
            return get_field_value_from_record(field=field, record=item, default=default, skip_missing=True)
        elif self == ItemType.SchemaRow:
            return get_field_value_from_schema_row(field=field, row=item, default=default, skip_missing=False)
        elif skip_unsupported_types:
            return default
        else:
            raise TypeError('type {} not supported'.format(self.get_name()))

    def set_to_item_inplace(self, field: Field, value: Value, item: Item) -> NoReturn:
        if self == ItemType.Record:
            item[field] = value
        elif self == ItemType.Row:
            cols_count = len(item)
            if field >= cols_count:
                item += [None] * (field - cols_count + 1)
            item[field] = value
        elif self == ItemType.SchemaRow:
            item.set_value(field, value)
        else:  # item_type == 'any' or not item_type:
            raise TypeError('type {} not supported'.format(self))


ItemType.prepare()
ItemType.set_default(ItemType.Auto)


def get_field_value_from_schema_row(field: Field, row: StructRowInterface, default=None, skip_missing=True):
    if isinstance(field, Callable):
        return field(row)
    else:
        field = arg.get_name(field)
    return row.get_value(field, default=default, skip_missing=skip_missing)


def get_field_value_from_row(column: int, row: Row, default=None, skip_missing=True):
    if column < len(row) or not skip_missing:
        return row[column]
    else:
        return default


def get_field_value_from_record(field: Field, record: Record, default=None, skip_missing=True):
    if isinstance(field, Callable):
        return field(record)
    else:
        field = arg.get_name(field)
    if skip_missing:
        return record.get(field, default)
    else:
        return record[field]
