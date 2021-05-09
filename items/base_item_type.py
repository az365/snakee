from typing import Union, Callable

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from utils.enum import DynamicEnum
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..utils.enum import DynamicEnum

Field = Union[int, str, Callable]
SimpleItem = Union[dict, list, tuple]


class ItemType(DynamicEnum):
    Line = 'line'
    Row = 'row'
    Record = 'record'
    SchemaRow = 'schema_row'
    Any = 'any'
    Auto = arg.DEFAULT

    @staticmethod
    def _get_selectable_types():
        return ItemType.Record, ItemType.Row, ItemType.SchemaRow

    def is_selectable(self):
        return self in self._get_selectable_types()

    def get_field_value_from_item(self, field: Field, item: SimpleItem, skip_errors: bool = True):
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

    def set_to_item_inplace(self, field, value, item):
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
