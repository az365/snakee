from enum import Enum

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg


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
