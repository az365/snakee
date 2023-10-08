from typing import Optional, Callable, Union, Any

try:  # Assume we're a submodule in a package.
    from base.classes.enum import SubclassesType
    from base.constants.chars import STAR, EMPTY, MINUS
    from base.functions.arguments import get_names
    from utils.decorators import deprecated_with_alternative
    from content.fields.field_interface import FieldInterface
    from content.struct.struct_interface import StructInterface
    from content.items.simple_items import (
        ROW_SUBCLASSES, RECORD_SUBCLASSES, FULL_ITEM_FIELD,
        SimpleItem, FieldNo, FieldName, FieldID, Value,
        get_field_value_from_record, get_field_value_from_row,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.enum import SubclassesType
    from ...base.constants.chars import STAR, EMPTY, MINUS
    from ...base.functions.arguments import get_names
    from ...utils.decorators import deprecated_with_alternative
    from ..fields.field_interface import FieldInterface
    from ..struct.struct_interface import StructInterface
    from .simple_items import (
        ROW_SUBCLASSES, RECORD_SUBCLASSES, FULL_ITEM_FIELD,
        SimpleItem, FieldNo, FieldName, FieldID, Value,
        get_field_value_from_record, get_field_value_from_row,
    )

RegularItem = SimpleItem
Item = Union[RegularItem, Any]
Field = Union[FieldID, FieldInterface]
Struct = Optional[StructInterface]


class ItemType(SubclassesType):
    Line = 'line'
    Row = 'row'
    Record = 'record'
    Paragraph = 'paragraph'
    Sheet = 'sheet'
    Any = 'any'
    Auto = None

    _auto_value = False  # option: do not update auto-value for ItemType.Auto

    @staticmethod
    def _get_selectable_types() -> tuple:
        return ItemType.Record, ItemType.Row

    def is_selectable(self) -> bool:
        return self in self._get_selectable_types()

    @deprecated_with_alternative('ItemType.get_field_getter()')
    def get_value_from_item(
            self,
            item: RegularItem,
            field: Field,
            struct: Struct = None,
            default: Value = None,
            skip_unsupported_types: bool = False,
    ) -> Value:
        if struct is not None:
            if self == ItemType.Row:
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
        elif skip_unsupported_types:
            return default
        else:
            raise TypeError(f'type {self.get_name()} not supported')

    def get_field_getter(self, field: Union[Field, Callable], struct: Struct = None, default: Value = None) -> Callable:
        """
        Returns field getter for one simple field.

        Used in ItemType.get_single_mapper(), ColumnarMixin._get_field_getter().
        ColumnarMixin._get_field_getter() used in ColumnarMixin.get_dict(), RowStream.sorted_group_by()
        """
        if struct is not None:
            if self == ItemType.Row:
                if isinstance(field, str):
                    field = struct.get_field_position(field)
        if hasattr(field, 'get_mapper'):  # isinstance(field, AbstractDescription)
            try:
                return field.get_mapper(struct=struct, item_type=self)
            except TypeError:
                return field.get_mapper()
        elif self == ItemType.Record:
            return lambda i: get_field_value_from_record(field=field, record=i, default=default, skip_missing=True)
        elif self == ItemType.Row:
            return lambda i: get_field_value_from_row(column=field, row=i, default=default, skip_missing=False)
        elif isinstance(field, Callable):
            return field
        elif field in (STAR, FULL_ITEM_FIELD):
            return lambda i: i
        elif field is None or field in (MINUS, EMPTY):
            return lambda i: None
        else:
            raise TypeError(f'type {self.get_name()} not supported')

    def get_single_mapper(self, *fields, function: Callable = tuple, struct: Struct = None) -> Callable:
        """
        Returns value getter for selection tuple (function and fields for its arguments).

        Used in AbstractDescription.get_mapper().
        """
        arg_getters = [self.get_field_getter(f, struct=struct) for f in fields]
        return lambda i: function(*[g(i) for g in arg_getters])

    def get_key_function(self, *fields, struct: Struct = None, take_hash: bool = False) -> Callable:
        """
        Returns key getter for single field or tuple of fields.
        Key getter returns single value or tuple of values from item.
        For use in *Stream.[sorted_]group_by().
        """
        fields = get_names(fields, or_callable=True)
        if len(fields) == 0:
            raise ValueError('key must be defined')
        elif len(fields) == 1:
            key_function = self.get_field_getter(fields[0], struct=struct)
        else:
            key_function = self.get_single_mapper(*fields, function=lambda *a: tuple(a), struct=struct)
        if take_hash:
            return lambda r: hash(key_function(r))
        else:
            return key_function

    def set_to_item_inplace(self, field: Field, value: Any, item: Item) -> None:
        if self == ItemType.Record:
            item[field] = value
        elif self == ItemType.Row:
            cols_count = len(item)
            if field >= cols_count:
                item += [None] * (field - cols_count + 1)
            item[field] = value
        else:  # item_type == 'any' or not item_type:
            raise TypeError(f'type {self} not supported')


ItemType.prepare()
ItemType.set_default(ItemType.Auto)
