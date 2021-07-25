from typing import Iterable, Union, Any, NoReturn

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from items.item_type import ItemType
    from items.struct_row_interface import StructRowInterface
    from items import legacy_classes as sc
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from . import arguments as arg
    from ..items.item_type import ItemType
    from ..items.struct_row_interface import StructRowInterface
    from ..items import legacy_classes as sc

Array = Union[list, tuple]
Row = Array
Record = dict
Line = str
SelectableItem = Union[Row, Record, StructRowInterface]
ConcreteItem = Union[Line, SelectableItem]
FieldName = str
FieldNo = int
FieldID = Union[FieldNo, FieldName]

STAR = '*'
ROW_SUBCLASSES = (list, tuple)


ItemType.prepare()
ItemType.set_dict_classes(
    {
        ItemType.Line: [Line],
        ItemType.Row: [*ROW_SUBCLASSES],
        ItemType.Record: [Record],
        ItemType.StructRow: [sc.StructRow, StructRowInterface],
    }
)


def set_to_item_inplace(field, value, item: SelectableItem, item_type=ItemType.Auto) -> NoReturn:
    item_type = arg.delayed_undefault(item_type, ItemType.detect, item, default=ItemType.Any)
    if not isinstance(item_type, ItemType):
        if hasattr(item_type, 'value'):
            item_type = ItemType(item_type.value)
        else:
            item_type = ItemType(item_type)
    if item_type == ItemType.Record:
        item[field] = value
    elif item_type == ItemType.Row:
        cols_count = len(item)
        if field >= cols_count:
            item += [None] * (field - cols_count + 1)
        item[field] = value
    elif item_type == ItemType.StructRow:
        item.set_value(field, value)
    else:  # item_type == 'any' or not item_type:
        raise TypeError('type {} not supported'.format(item_type))


def get_fields_names_from_item(item: SelectableItem, item_type=ItemType.Auto) -> Row:
    item_type = arg.delayed_undefault(item_type, ItemType.detect, item, default=ItemType.Any)
    if item_type == ItemType.Row:
        return list(range(len(item)))
    elif item_type == ItemType.Record:
        return item.keys()
    elif item_type == ItemType.StructRow:
        return item.get_columns()
    else:
        raise TypeError('type {} not supported'.format(item_type))


def get_field_value_from_schema_row(key: FieldID, row: StructRowInterface, default=None, skip_missing=True):
    return row.get_value(key, default=default, skip_missing=skip_missing)


def get_field_value_from_row(column: int, row: Row, default=None, skip_missing=True):
    if column < len(row) or not skip_missing:
        return row[column]
    else:
        return default


def get_field_value_from_record(field: FieldID, record: Record, default=None, skip_missing=True):
    if skip_missing:
        return record.get(field, default)
    else:
        return record[field]


def get_field_value_from_item(field, item, item_type=ItemType.Auto, skip_errors=False, logger=None, default=None):
    if field == STAR:
        return item
    if item_type == ItemType.Auto or not arg.is_defined(item_type):
        item_type = ItemType.detect(item, default='any')
    if isinstance(item_type, str):
        item_type = ItemType(item_type)
    else:
        item_type = ItemType(item_type.value)
    try:
        return item_type.get_value_from_item(
            item=item, field=field, default=default, skip_unsupported_types=skip_errors,
        )
    except IndexError or TypeError:
        msg = 'Field {} does no exists in current item'.format(field)
        if skip_errors:
            if logger:
                logger.log(msg)
            return default
        else:
            raise IndexError(msg)


def get_fields_values_from_item(
        fields: Array, item: SelectableItem, item_type=ItemType.Auto,
        skip_errors=False, logger=None, default=None,
) -> list:
    return [get_field_value_from_item(f, item, item_type, skip_errors, logger, default) for f in fields]


def simple_select_fields(fields: Array, item: SelectableItem, item_type=ItemType.Auto) -> SelectableItem:
    item_type = arg.delayed_undefault(item_type, ItemType.detect, item, default=ItemType.Any)
    if isinstance(item_type, str):
        item_type = ItemType(item_type)
    if item_type == ItemType.Record:
        return {f: item.get(f) for f in fields}
    elif item_type == ItemType.Row:
        return [item[f] for f in fields]
    elif item_type == ItemType.StructRow:
        return item.simple_select_fields(fields)


def get_values_by_keys_from_item(  # equivalent get_fields_values_from_items()
        item: SelectableItem, keys: Iterable, default=None,
) -> list:
    return [get_value_by_key_from_item(item, k, default) for k in keys]


def get_value_by_key_from_item(item, key, default=None) -> Any:  # equivalent get_field_value_from_item()
    if ItemType.Record.isinstance(item):
        return item.get(key, default)
    elif ItemType.Row.isinstance(item):
        return item[key] if isinstance(key, int) and 0 <= key <= len(item) else default


def get_copy(item) -> Any:
    if isinstance(item, tuple):
        return list(item)
    else:
        return item.copy()


def get_frozen(item) -> Any:
    if isinstance(item, list):
        return tuple(item)
    else:
        return item
