from typing import Optional, Callable, Union

try:  # Assume we're a submodule in a package.
    from base.classes.typing import Array, ARRAY_TYPES
    from base.constants.chars import STAR
    from base.functions.arguments import get_names, update
    from base.functions.errors import get_type_err_msg
    from base.classes.typing import FieldNo, FieldName
    from loggers.logger_interface import LoggerInterface
    from functions.primary.items import get_fields_values_from_item, get_field_value_from_item
    from content.selection.selection_functions import (
        Description, safe_apply_function,
        process_description, flatten_descriptions, support_simple_filter_expressions
    )
    from content.items.simple_items import Record, MutableRecord, Row, ImmutableRow, Item, Value
    from content.items.item_type import ItemType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.typing import Array, ARRAY_TYPES
    from ...base.constants.chars import STAR
    from ...base.functions.arguments import get_names, update
    from ...base.functions.errors import get_type_err_msg
    from ...base.classes.typing import FieldNo, FieldName
    from ...loggers.logger_interface import LoggerInterface
    from ...functions.primary.items import get_fields_values_from_item, get_field_value_from_item
    from ..selection.selection_functions import (
        Description, safe_apply_function,
        process_description, flatten_descriptions, support_simple_filter_expressions
    )
    from .simple_items import Record, MutableRecord, Row, ImmutableRow, Item, Value
    from .item_type import ItemType


def value_from_row(row: Row, description: Description, logger=None, skip_errors=True) -> Value:
    if isinstance(description, Callable):
        return description(row)
    elif isinstance(description, ARRAY_TYPES):
        function, columns = process_description(description)
        values = [row[f] for f in columns]
        return safe_apply_function(function, columns, values, item=row, logger=logger, skip_errors=skip_errors)
    elif isinstance(description, FieldNo):
        return row[description]
    else:
        msg = get_type_err_msg(description, expected=(int, Callable, tuple), arg='description', caller=value_from_row)
        raise TypeError(msg)


# @deprecated
def value_from_struct_row(row, description: Description, logger=None, skip_errors=True) -> Value:
    if isinstance(description, Callable):
        return description(row)
    elif isinstance(description, (FieldNo, FieldName)):
        return row.get_value(description)
    elif isinstance(description, ARRAY_TYPES):
        function, fields = process_description(description)
        values = [row.get_value(c) for c in fields]
        return safe_apply_function(function, fields, values, item=row, logger=logger, skip_errors=skip_errors)


def value_from_record(record: Record, description: Description, logger=None, skip_errors=True) -> Value:
    if isinstance(description, Callable):
        return description(record)
    elif isinstance(description, ARRAY_TYPES):
        function, fields = process_description(description)
        values = [record.get(f) for f in fields]
        return safe_apply_function(function, fields, values, item=record, logger=logger, skip_errors=skip_errors)
    elif hasattr(description, 'get_names'):
        return [record.get(n) for n in description.get_names()]
    elif hasattr(description, 'get_name'):
        return record.get(description.get_name())
    else:
        return record.get(description)


def value_from_any(item: Item, description: Description, logger=None, skip_errors=True) -> Value:
    if isinstance(description, Callable):
        return description(item)
    elif isinstance(description, ARRAY_TYPES):
        function, fields = process_description(description)
        values = get_fields_values_from_item(fields, item)
        return safe_apply_function(function, fields, values, item=item, logger=logger, skip_errors=skip_errors)
    else:
        return get_field_value_from_item(description, item)


def value_from_item(
        item: Item,
        description: Description,
        item_type: ItemType = ItemType.Auto,
        logger: Optional[LoggerInterface] = None,
        skip_errors: bool = True,
        default: Value = None,
) -> Value:
    if hasattr(description, 'get_mapper'):
        try:
            description = description.get_mapper(item_type=item_type)
        except TypeError:
            description = description.get_mapper()
    elif hasattr(description, 'get_name'):
        description = description.get_name()
    if isinstance(description, Callable):
        return description(item)
    elif isinstance(description, (FieldNo, FieldName)):
        return get_field_value_from_item(
            field=description, item=item, item_type=item_type,
            skip_errors=skip_errors, logger=logger, default=default,
        )
    elif isinstance(description, ARRAY_TYPES):
        function, fields = process_description(description)
        fields = get_names(fields, or_callable=True)
        values = get_fields_values_from_item(
            fields, item, item_type=item_type,
            skip_errors=skip_errors, logger=logger, default=default,
        )
        return safe_apply_function(function, fields, values, item=item, logger=logger, skip_errors=skip_errors)
    else:
        msg = get_type_err_msg(description, expected=(int, Callable, tuple), arg='description', caller=value_from_item)
        raise TypeError(msg)


def get_composite_key(
        item: Item,
        keys_descriptions: Array,
        item_type: ItemType = ItemType.Any,
        logger: Optional[LoggerInterface] = None,
        skip_errors: bool = True,
) -> ImmutableRow:
    keys_descriptions = update(keys_descriptions)
    keys_descriptions = [d.get_field_names() if hasattr(d, 'get_field_names') else d for d in keys_descriptions]
    result = list()
    for d in keys_descriptions:
        if isinstance(d, Callable):
            value = d(item)
        else:
            value = value_from_item(item, d, item_type=item_type, logger=logger, skip_errors=skip_errors)
        result.append(value)
    return ImmutableRow(result)


def tuple_from_record(record: Record, descriptions: Array, logger=None) -> tuple:
    return tuple([value_from_record(record, d, logger=logger) for d in descriptions])


def row_from_row(row_in: Row, *descriptions) -> ImmutableRow:
    row_out = [None] * len(descriptions)
    c = 0
    for d in descriptions:
        if d == STAR:
            row_out = row_out[:c] + list(row_in) + row_out[c + 1:]
            c += len(row_in)
        else:
            row_out[c] = value_from_row(row_in, d)
            c += 1
    return ImmutableRow(row_out)


def row_from_any(item_in: Item, *descriptions) -> ImmutableRow:
    row_out = [None] * len(descriptions)
    c = 0
    for desc in descriptions:
        if desc == STAR:
            if ItemType.Row.isinstance(item_in):
                row_out = row_out[:c] + list(item_in) + row_out[c + 1:]
                c += len(item_in)
            else:
                row_out[c] = item_in
                c += 1
        else:
            row_out[c] = value_from_any(item_in, desc)
            c += 1
    return ImmutableRow(row_out)


def record_from_any(item_in: Item, *descriptions, logger=None) -> MutableRecord:
    rec_out = MutableRecord()
    for desc in descriptions:
        if isinstance(desc, ARRAY_TYPES):
            if len(desc) < 2:
                raise ValueError(f'for RegularStream items description must have 2+ parts, got {desc}')
            else:
                f_out = desc[0]
                if len(desc) == 2:
                    f_in = desc[1]
                    if isinstance(f_in, Callable):
                        rec_out[f_out] = f_in(item_in)
                    else:
                        rec_out[f_out] = rec_out.get(f_in)
                else:
                    fs_in = desc[1:]
                    rec_out[f_out] = value_from_record(rec_out, fs_in, logger=logger)
        else:
            msg = get_type_err_msg(expected=tuple, got=desc, arg='descriptions[n]', caller=record_from_any)
            raise TypeError(msg)
    return rec_out


def record_from_record(rec_in: MutableRecord, *descriptions, logger: Optional[LoggerInterface] = None) -> MutableRecord:
    record = rec_in.copy()
    fields_out = list()
    for desc in descriptions:
        if desc == STAR:
            fields_out += list(rec_in.keys())
        elif isinstance(desc, ARRAY_TYPES):
            if len(desc) > 1:
                f_out = desc[0]
                fs_in = desc[1] if len(desc) == 2 else desc[1:]
                record[f_out] = value_from_record(record, fs_in, logger=logger)
                fields_out.append(f_out)
            else:
                raise ValueError('incorrect field description: {}'.format(desc))
        else:  # desc is field name
            if hasattr(desc, 'get_name'):  # isinstance(desc, FieldInterface)
                desc = desc.get_name()
            if desc not in record:
                record[desc] = None
            fields_out.append(desc)
    return {f: record[f] for f in fields_out}


def auto_to_auto(item: Item, *descriptions, logger: Optional[LoggerInterface] = None) -> Item:
    item_type = ItemType.detect(item, default=ItemType.Any)
    if item_type == ItemType.Record:
        return record_from_record(item, *descriptions, logger=logger)
    elif item_type == ItemType.Row:
        return row_from_row(item, *descriptions)
    else:
        return get_composite_key(item, descriptions)


def get_selection_mapper(
        *fields,
        target_item_type: ItemType = ItemType.Auto,
        input_item_type: ItemType = ItemType.Auto,
        logger: Optional[LoggerInterface] = None,
        selection_logger: Optional[LoggerInterface] = None,
        **expressions
) -> Callable:
    descriptions = flatten_descriptions(*fields, **expressions, logger=logger)
    if target_item_type == ItemType.Record:
        if input_item_type == ItemType.Record:
            return lambda r: record_from_record(r, *descriptions, logger=selection_logger)
        elif input_item_type == ItemType.Any:
            return lambda i: record_from_any(i, *descriptions, logger=logger)
    elif target_item_type == ItemType.Row:
        if input_item_type == ItemType.Row:
            return lambda r: row_from_row(r, *descriptions)
        elif input_item_type == ItemType.Any:
            return lambda i: row_from_any(i, *descriptions)
    return lambda i: auto_to_auto(i, *descriptions, logger=logger)


def get_filter_function(
        *fields,
        item_type: ItemType = ItemType.Auto,
        skip_errors: bool = False,
        logger: Optional[LoggerInterface] = None,
        **expressions
) -> Callable:
    extended_filters_list = support_simple_filter_expressions(*fields, **expressions)
    return lambda i: apply_filter_list_to_item(
        item=i, filter_list=extended_filters_list,
        item_type=item_type, skip_errors=skip_errors, logger=logger,
    )


def apply_filter_list_to_item(
        item,
        filter_list: Array,
        item_type: ItemType = ItemType.Auto,
        skip_errors: bool = False,
        logger: Optional[LoggerInterface] = None,
) -> bool:
    for filter_desc in filter_list:
        if not value_from_item(item, filter_desc, item_type=item_type, logger=logger, skip_errors=skip_errors):
            return False
    return True
