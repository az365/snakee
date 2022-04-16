from typing import Optional, Callable, Union, Any

try:  # Assume we're a submodule in a package.
    from base.classes.enum import SubclassesType
    from utils.decorators import deprecated_with_alternative
    from content.fields.field_interface import FieldInterface
    from content.struct.struct_interface import StructInterface
    from content.struct.struct_row_interface import StructRowInterface
    from content.items.simple_items import (
        STAR, ROW_SUBCLASSES, RECORD_SUBCLASSES, AUTO, Auto,
        SimpleItem, FieldNo, FieldName, FieldID, Value, Array,
        get_field_value_from_record, get_field_value_from_row, get_field_value_from_struct_row,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.enum import SubclassesType
    from ...utils.decorators import deprecated_with_alternative
    from ..fields.field_interface import FieldInterface
    from ..struct.struct_interface import StructInterface
    from ..struct.struct_row_interface import StructRowInterface
    from .simple_items import (
        STAR, ROW_SUBCLASSES, RECORD_SUBCLASSES, AUTO, Auto,
        SimpleItem, FieldNo, FieldName, FieldID, Value, Array,
        get_field_value_from_record, get_field_value_from_row, get_field_value_from_struct_row,
    )

RegularItem = Union[SimpleItem, StructRowInterface]
Item = Union[RegularItem, Any]
Field = Union[FieldID, FieldInterface]


class ItemType(SubclassesType):
    Line = 'line'
    Row = 'row'
    Record = 'record'
    StructRow = 'struct_row'
    Any = 'any'
    Auto = AUTO

    _auto_value = False  # option: do not update auto-value for ItemType.Auto

    @staticmethod
    def _get_selectable_types() -> tuple:
        return ItemType.Record, ItemType.Row, ItemType.StructRow

    def is_selectable(self) -> bool:
        return self in self._get_selectable_types()

    @deprecated_with_alternative('ItemType.get_value_from_item()')
    def get_field_value_from_item(self, field: Field, item: Item, skip_errors: bool = True) -> Value:
        if isinstance(field, Callable):
            return field(item)
        if skip_errors:
            is_row = self == ItemType.Row or isinstance(field, FieldNo) or isinstance(item, ROW_SUBCLASSES)
            is_record = self == ItemType.Record or isinstance(field, FieldName) or isinstance(item, RECORD_SUBCLASSES)
            if is_row:
                if field < len(item):
                    return item[field]
            elif is_record:
                return item.get(field)
        else:
            is_row = self == ItemType.Row and isinstance(field, FieldNo) and isinstance(item, ROW_SUBCLASSES)
            is_record = self == ItemType.Record and isinstance(field, FieldName) and isinstance(item, RECORD_SUBCLASSES)
            assert is_row or is_record
            return item[field]

    def get_value_from_item(
            self,
            item: RegularItem, field: Field,
            struct: Optional[StructInterface] = None,
            default: Value = None, skip_unsupported_types: bool = False,
    ) -> Value:
        if Auto.is_defined(struct):
            if self in (ItemType.Row, ItemType.StructRow):
                if isinstance(field, str):
                    field = struct.get_field_position(field)
        if self == ItemType.Auto:
            item_type = self.detect(item, default=ItemType.Any)
            assert isinstance(item_type, ItemType)
            return item_type.get_value_from_item(item, field, default)
        elif self == ItemType.Row:
            return get_field_value_from_row(column=field, row=item, default=default, skip_missing=False)
        elif self == ItemType.Record:
            return get_field_value_from_record(field=field, record=item, default=default, skip_missing=True)
        elif self == ItemType.StructRow:
            return get_field_value_from_struct_row(field=field, row=item, default=default, skip_missing=False)
        elif skip_unsupported_types:
            return default
        else:
            raise TypeError('type {} not supported'.format(self.get_name()))

    def set_to_item_inplace(self, field: Field, value: Any, item: Item) -> None:
        if self == ItemType.Record:
            item[field] = value
        elif self == ItemType.Row:
            cols_count = len(item)
            if field >= cols_count:
                item += [None] * (field - cols_count + 1)
            item[field] = value
        elif self == ItemType.StructRow:
            item.set_value(field, value)
        else:  # item_type == 'any' or not item_type:
            raise TypeError('type {} not supported'.format(self))


ItemType.prepare()
ItemType.set_default(ItemType.Auto)
