from typing import Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from schema.schema_classes import SchemaRow
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from . import arguments as arg
    from ..schema.schema_classes import SchemaRow


def build_item(item_type):
    if item_type == 'line':
        return str()
    elif item_type == 'record':
        return dict()
    elif item_type == 'row':
        return list()
    elif item_type == 'schema_row':
        return SchemaRow([], [])
    elif callable(item_type):
        return item_type()


def detect_item_type(item):
    if is_row(item):
        return 'row'
    elif is_record(item):
        return 'record'
    elif is_schema_row(item):
        return 'schema_row'


def is_row(item):
    return isinstance(item, (list, tuple))


def is_record(item):
    return isinstance(item, dict)


def is_schema_row(item):
    return isinstance(item, SchemaRow)


def set_to_item_inplace(field, value, item, item_type=arg.DEFAULT):
    item_type = arg.undefault(item_type, detect_item_type(item))
    if item_type in ['row', 'record']:
        item[field] = value
    elif item_type == 'schema_row':
        item.set_value(field, value)
    else:
        raise TypeError('type {} not supported'.format(item_type))


def get_field_value_from_schema_row(key: Union[int, str], row: SchemaRow):
    return row.get_value(key)


def get_field_value_from_row(column: int, row: Union[tuple, list]):
    return row[column]


def get_field_value_from_record(field: Union[str, int], record: dict, default=None):
    return record.get(field, default)


def get_field_value_from_item(field, item, item_type=None, skip_errors=False, logger=None, default=None):
    if not item_type:
        item_type = detect_item_type(item)
    method_callable = getattr('get_field_value_from_{}'.format(item_type))
    try:
        method_callable(field, item)
    except IndexError or TypeError:
        msg = 'Field {} does no exists in current item'.format(field)
        if skip_errors:
            if logger:
                logger.log(msg)
            return default
        else:
            raise IndexError(msg)


def get_fields_values_from_item(
        fields: Union[tuple, list], item: Union[tuple, list, SchemaRow], item_type=None,
        skip_errors=False, logger=None, default=None,
):
    return [get_field_value_from_item(f, item, item_type, skip_errors, logger, default) for f in fields]
