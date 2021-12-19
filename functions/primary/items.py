from typing import Optional, Iterable, Callable, Union, Any

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from items.item_type import ItemType
    from items.struct_row_interface import StructRowInterface
    from items.simple_items import (
        Row, Record, Line, SimpleSelectableItem,
        STAR, ROW_SUBCLASSES, RECORD_SUBCLASSES,
        FieldNo, FieldName, FieldID, Value, Array, ARRAY_TYPES,
        get_field_value_from_row, get_field_value_from_record,
        merge_two_rows, merge_two_records,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...items.item_type import ItemType
    from ...items.struct_row_interface import StructRowInterface
    from ...items.simple_items import (
        Row, Record, Line, SimpleSelectableItem,
        STAR, ROW_SUBCLASSES, RECORD_SUBCLASSES,
        FieldNo, FieldName, FieldID, Value, Array, ARRAY_TYPES,
        get_field_value_from_row, get_field_value_from_record,
        merge_two_rows, merge_two_records,
    )

SelectableItem = Union[SimpleSelectableItem, StructRowInterface]
ConcreteItem = Union[Line, SelectableItem]


def set_to_item_inplace(
        field: FieldID, value: Value,
        item: SelectableItem, item_type: ItemType = ItemType.Auto,
) -> None:
    item_type = arg.delayed_acquire(item_type, ItemType.detect, item, default=ItemType.Any)
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
        if isinstance(item, StructRowInterface):
            item.set_value(field, value, update_struct=True)
        elif isinstance(item, ROW_SUBCLASSES):
            assert isinstance(field, int), 'Expected column number as int, got {}'.format(field)
            cur_item_len = len(item)
            need_extend = field >= cur_item_len
            if need_extend:
                if isinstance(item, tuple):
                    item = list(item)
                item += [None] * (field + 1 - cur_item_len)
            item[field] = value
        else:
            raise TypeError('Expected Row or StructRow, got {}'.format(item))
    else:  # item_type == 'any' or not item_type:
        raise TypeError('type {} not supported'.format(item_type))


def get_fields_names_from_item(item: SelectableItem, item_type: ItemType = ItemType.Auto) -> Row:
    item_type = arg.delayed_acquire(item_type, ItemType.detect, item, default=ItemType.Any)
    if item_type == ItemType.Row:
        return list(range(len(item)))
    elif item_type == ItemType.Record:
        return item.keys()
    elif item_type == ItemType.StructRow:
        return item.get_columns()
    else:
        raise TypeError('type {} not supported'.format(item_type))


def get_field_value_from_item(
        field: Union[FieldID, Array, Callable],
        item: ConcreteItem, item_type: ItemType = ItemType.Auto,
        skip_errors: bool = False, logger=None, default: Value = None,
) -> Value:
    if field == STAR:
        return item
    elif isinstance(field, Callable):
        return field(item)
    elif isinstance(field, ARRAY_TYPES):
        list_values = get_fields_values_from_item(
            field,
            item=item, item_type=item_type,
            default=default,
            skip_errors=skip_errors, logger=logger,
        )
        return tuple(list_values)
    if item_type == ItemType.Auto or not arg.is_defined(item_type):
        item_type = ItemType.detect(item, default='any')
    elif isinstance(item_type, str):
        item_type = ItemType(item_type)
    elif hasattr(item_type, 'get_value'):
        item_type = ItemType(item_type.get_value())
    elif hasattr(item_type, 'value'):
        item_type = ItemType(item_type.value)
    else:
        msg = 'get_field_value_from_item(item, item_type): expected item_type as ItemType, got {}'
        raise TypeError(msg.format(item_type))
    try:
        return item_type.get_value_from_item(
            item=item, field=field,
            default=default, skip_unsupported_types=skip_errors,
        )
    except IndexError as e:
        msg = '{}: Field {} does no exists in current item ({}): {}'.format('IndexError', field, e, item)
    except TypeError as e:
        msg = '{}: Field {} does no exists in current item ({}): {}'.format('TypeError', field, e, item)
    if skip_errors:
        if logger:
            logger.log(msg)
        return default
    else:
        raise IndexError(msg)


def get_fields_values_from_item(
        fields: Array, item: SelectableItem, item_type=ItemType.Auto,
        skip_errors: bool = False, logger=None, default: Value = None,
) -> list:
    return [get_field_value_from_item(f, item, item_type, skip_errors, logger, default) for f in fields]


def simple_select_fields(fields: Array, item: SelectableItem, item_type: ItemType = ItemType.Auto) -> SelectableItem:
    item_type = arg.delayed_acquire(item_type, ItemType.detect, item, default=ItemType.Any)
    if isinstance(item_type, str):
        item_type = ItemType(item_type)
    if item_type == ItemType.Record:
        return {f: item.get(f) for f in fields}
    elif item_type == ItemType.Row:
        return [item[f] for f in fields]
    elif item_type == ItemType.StructRow:
        return item.simple_select_fields(fields)


def get_values_by_keys_from_item(  # equivalent get_fields_values_from_items()
        item: SelectableItem, keys: Iterable, default: Value = None,
) -> list:
    return [get_value_by_key_from_item(item, k, default) for k in keys]


def get_value_by_key_from_item(  # equivalent get_field_value_from_item()
        item: ConcreteItem, key: FieldID, default: Value = None,
) -> Any:
    if ItemType.Record.isinstance(item):
        return item.get(key, default)
    elif ItemType.Row.isinstance(item):
        return item[key] if isinstance(key, int) and 0 <= key <= len(item) else default


def get_copy(item: ConcreteItem) -> ConcreteItem:
    if isinstance(item, tuple):
        return list(item)
    else:
        return item.copy()


def get_frozen(item: ConcreteItem) -> ConcreteItem:
    if isinstance(item, list):
        return tuple(item)
    else:
        return item


def merge_two_items(first: ConcreteItem, second: ConcreteItem, default_right_name: str = '_right') -> ConcreteItem:
    if ItemType.Row.isinstance(first):
        result = merge_two_rows(first, second)
    elif ItemType.Record.isinstance(first):
        result = merge_two_records(first, second, default_right_name=default_right_name)
    elif first is None and ItemType.Record.isinstance(second):
        result = second
    else:
        result = first, second
    return result


def items_to_dict(
        items: Iterable,
        key_function: Callable,
        value_function: Optional[Callable] = None,
        of_lists: bool = False,
) -> dict:
    result = dict()
    for i in items:
        k = key_function(i)
        v = i if value_function is None else value_function(i)
        if of_lists:
            distinct = result.get(k, [])
            if v not in distinct:
                result[k] = distinct + [v]
        else:
            result[k] = v
    return result
