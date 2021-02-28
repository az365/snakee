from enum import Enum
from typing import Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from schema.schema_classes import SchemaRow
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from . import arguments as arg
    from ..schema.schema_classes import SchemaRow


STAR = '*'

Array = Union[list, tuple]
Row = Array
Record = dict
Line = str
SelectableItem = Union[Row, Record, SchemaRow]
ConcreteItem = Union[Line, SelectableItem]
FieldName = str
FieldNo = int
FieldID = Union[FieldNo, FieldName]


class ItemType(Enum):
    Line = 'line'
    Row = 'row'
    Record = 'record'
    SchemaRow = 'schema_row'
    Any = 'any'
    Auto = arg.DEFAULT

    @staticmethod
    def get_dict_subclasses():
        return {
            ItemType.Line: [str],
            ItemType.Row: [list, tuple],
            ItemType.Record: [dict],
            ItemType.SchemaRow: [SchemaRow],
        }

    def get_subclasses(self):
        return tuple(
            self.get_dict_subclasses().get(self, [None])
        )

    def get_builder(self):
        return self.get_subclasses()[0]

    def build(self) -> ConcreteItem:
        builder = self.get_builder()
        if builder:
            return builder()

    def isinstance(self, item):
        subclasses = self.get_subclasses()
        return isinstance(item, tuple(subclasses))

    @staticmethod
    def detect(item):
        for item_type in ItemType.get_dict_subclasses():
            if item_type.isinstance(item):
                return item_type
        else:
            return ItemType.Any

    @staticmethod
    def get_selectable_types():
        return ItemType.Record, ItemType.Row, ItemType.SchemaRow

    def is_selectable(self):
        return self in self.get_selectable_types()

    def get_value_from_item(self, item, field: FieldID, default=None):
        if self == ItemType.Row:
            return get_field_value_from_row(column=field, row=item, default=default)
        elif self == ItemType.Record:
            return get_field_value_from_record(field=field, record=item, default=default)
        elif self == ItemType.SchemaRow:
            return get_field_value_from_schema_row(key=field, row=item, default=default)
        else:
            return default


def set_to_item_inplace(field, value, item: SelectableItem, item_type=ItemType.Auto):
    item_type = ItemType(arg.undefault(item_type, ItemType.detect(item)))
    if item_type == ItemType.Record:
        item[field] = value
    elif item_type == ItemType.Row:
        cols_count = len(item)
        if field >= cols_count:
            item += [None] * (field - cols_count + 1)
        item[field] = value
    elif item_type == ItemType.SchemaRow:
        item.set_value(field, value)
    else:  # item_type == 'any' or not item_type:
        raise TypeError('type {} not supported'.format(item_type))


def get_fields_names_from_item(item: SelectableItem, item_type=ItemType.Auto) -> Row:
    item_type = arg.undefault(item_type, ItemType.detect(item))
    if item_type == ItemType.Row:
        return list(range(len(item)))
    elif item_type == ItemType.Record:
        return item.keys()
    elif item_type == ItemType.SchemaRow:
        return item.get_columns()
    else:
        raise TypeError('type {} not supported'.format(item_type))


def get_field_value_from_schema_row(key: FieldID, row: SchemaRow, default=None, skip_missing=True):
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
    if not item_type:
        item_type = ItemType.detect(item)
    try:
        return ItemType(item_type).get_value_from_item(item=item, field=field, default=default)
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
):
    return [get_field_value_from_item(f, item, item_type, skip_errors, logger, default) for f in fields]


def simple_select_fields(fields: Array, item: SelectableItem, item_type=ItemType.Auto):
    item_type = arg.undefault(item_type, ItemType.detect(item))
    if isinstance(item_type, str):
        item_type = ItemType(item_type)
    if item_type == ItemType.Record:
        return {f: item.get(f) for f in fields}
    elif item_type == ItemType.Row:
        return [item[f] for f in fields]
    elif item_type == ItemType.SchemaRow:
        return item.simple_select_fields(fields)


def get_frozen(item):
    if isinstance(item, list):
        return tuple(item)
    else:
        return item
