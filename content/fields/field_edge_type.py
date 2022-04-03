try:  # Assume we're a submodule in a package.
    from base.classes.enum import DynamicEnum
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.enum import DynamicEnum


class FieldEdgeType(DynamicEnum):
    Terms = 'terms'
    ItemTypes = 'item_types'
    Enum = 'enum'


FieldEdgeType.prepare()
