from typing import Optional, Callable, Union, Any

try:  # Assume we're a submodule in a package.
    from base.classes.enum import SubclassesType
    from base.constants.chars import STAR, EMPTY, MINUS
    from utils.decorators import deprecated_with_alternative
    from content.fields.field_interface import FieldInterface
    from content.struct.struct_interface import StructInterface
    from content.struct.struct_row_interface import StructRowInterface
    from content.items.simple_items import (
        ROW_SUBCLASSES, RECORD_SUBCLASSES, AUTO, Auto,
        SimpleItem, FieldNo, FieldName, FieldID, Value,
        get_field_value_from_record, get_field_value_from_row, get_field_value_from_struct_row,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.enum import SubclassesType
    from ...base.constants.chars import STAR, EMPTY, MINUS
    from ...utils.decorators import deprecated_with_alternative
    from ..fields.field_interface import FieldInterface
    from ..struct.struct_interface import StructInterface
    from ..struct.struct_row_interface import StructRowInterface
    from .simple_items import (
        ROW_SUBCLASSES, RECORD_SUBCLASSES, AUTO, Auto,
        SimpleItem, FieldNo, FieldName, FieldID, Value,
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

    # @deprecated_with_alternative('ItemType.get_field_getter()')
    def get_value_from_item(
            self,
            item: RegularItem,
            field: Field,
            struct: Optional[StructInterface] = None,
            default: Value = None,
            skip_unsupported_types: bool = False,
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

    def get_field_getter(
            self,
            field: Field,
            struct: Optional[StructInterface] = None,
            default: Value = None,
    ) -> Callable:
        if Auto.is_defined(struct):
            if self in (ItemType.Row, ItemType.StructRow):
                if isinstance(field, str):
                    field = struct.get_field_position(field)
        if self == ItemType.Record:
            return lambda i: get_field_value_from_record(field=field, record=i, default=default, skip_missing=True)
        elif self == ItemType.Row:
            return lambda i: get_field_value_from_row(column=field, row=i, default=default, skip_missing=False)
        elif self == ItemType.StructRow:
            return lambda i: get_field_value_from_struct_row(field=field, row=i, default=default, skip_missing=False)
        elif field == STAR:
            return lambda i: i
        elif field is None or field in (MINUS, EMPTY):
            return lambda i: None
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
