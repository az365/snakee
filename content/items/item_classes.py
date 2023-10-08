try:  # Assume we're a submodule in a package.
    from content.items.item_type import ItemType
    from content.items.simple_items import (
        FieldName, FieldNo, FieldID, Value, Class, Array, ARRAY_TYPES,
        ROW_SUBCLASSES, RECORD_SUBCLASSES, LINE_SUBCLASSES, ALL,
        Line,
        FrozenDict, SimpleRecord, MutableRecord, ImmutableRecord, Record,
        SimpleRowInterface, SimpleRow, MutableRow, ImmutableRow, Row,
        SimpleSelectableItem, SimpleItem, Item,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .item_type import ItemType
    from .simple_items import (
        FieldName, FieldNo, FieldID, Value, Class, Array, ARRAY_TYPES,
        ROW_SUBCLASSES, RECORD_SUBCLASSES, LINE_SUBCLASSES, ALL,
        Line,
        FrozenDict, SimpleRecord, MutableRecord, ImmutableRecord, Record,
        SimpleRowInterface, SimpleRow, MutableRow, ImmutableRow, Row,
        SimpleSelectableItem, SimpleItem, Item,
    )

ItemType.set_dict_subclasses(
    {
        ItemType.Any.get_value(): [object],
        ItemType.Line.get_value(): [str],
        ItemType.Record.get_value(): RECORD_SUBCLASSES,
        ItemType.Row.get_value(): ROW_SUBCLASSES,
    }
)
