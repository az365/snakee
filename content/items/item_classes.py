try:  # Assume we're a submodule in a package.
    from content.items.item_type import ItemType
    from content.items.simple_items import (
        FieldName, FieldNo, FieldID, Value, Class, Array, ARRAY_TYPES,
        ROW_SUBCLASSES, RECORD_SUBCLASSES, LINE_SUBCLASSES, STAR,
        SimpleRowInterface, SimpleRow, Row, Line, Record,
        SimpleSelectableItem, SimpleItem, Item,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .item_type import ItemType
    from .simple_items import (
        FieldName, FieldNo, FieldID, Value, Class, Array, ARRAY_TYPES,
        ROW_SUBCLASSES, RECORD_SUBCLASSES, LINE_SUBCLASSES, STAR,
        SimpleRowInterface, SimpleRow, Row, Line, Record,
        SimpleSelectableItem, SimpleItem, Item,
    )
