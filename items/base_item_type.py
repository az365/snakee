from enum import Enum
from typing import Union, Callable

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg

Field = Union[int, str, Callable]
SimpleItem = Union[dict, list, tuple]


class ItemType(Enum):
    Line = 'line'
    Row = 'row'
    Record = 'record'
    SchemaRow = 'schema_row'
    Any = 'any'
    Auto = arg.DEFAULT

    def get_value(self):
        return self.value

    def get_name(self):
        return self.get_name()

    @staticmethod
    def get_selectable_types():
        return ItemType.Record, ItemType.Row, ItemType.SchemaRow

    def is_selectable(self):
        return self in self.get_selectable_types()

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

