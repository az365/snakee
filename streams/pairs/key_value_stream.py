from typing import Optional, Callable, Iterable, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        RegularStreamInterface, PairStreamInterface, Struct, Connector, Context, TmpFiles,
        StreamType, ItemType,
        Name, Count,
    )
    from utils.decorators import deprecated, deprecated_with_alternative
    from content.struct.flat_struct import FlatStruct
    from functions.secondary import array_functions as fs
    from streams.regular.row_stream import RowStream
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        RegularStreamInterface, PairStreamInterface, Struct, Connector, Context, TmpFiles,
        StreamType, ItemType,
        Name, Count,
    )
    from ...functions.secondary import array_functions as fs
    from ...content.struct.flat_struct import FlatStruct
    from ...utils.decorators import deprecated, deprecated_with_alternative
    from ..regular.row_stream import RowStream

Native = PairStreamInterface
Stream = RegularStreamInterface

EXPECTED_ITEM_TYPE = ItemType.Row
KEY = fs.first()
VALUE = fs.second()


class KeyValueStream(RowStream, PairStreamInterface):
    @deprecated_with_alternative('RegularStream(item_type=ItemType.Row)')
    def __init__(
            self,
            data,
            name: Optional[Name] = None,
            caption: str = '',
            item_type: ItemType = EXPECTED_ITEM_TYPE,
            value_item_type: Union[StreamType, str] = None,
            struct: Struct = None,
            source: Connector = None,
            context: Context = None,
            count: Count = None,
            less_than: Count = None,
            max_items_in_memory: Count = None,
            tmp_files: TmpFiles = None,
            check: bool = True,
    ):
        super().__init__(
            data=data, check=check,
            item_type=item_type, struct=struct,
            source=source, context=context,
            name=name, caption=caption,
            count=count, less_than=less_than,
            max_items_in_memory=max_items_in_memory,
            tmp_files=tmp_files,
        )
        self.value_item_type = None
        self.set_value_item_type(value_item_type)

    @deprecated_with_alternative('Struct')
    def get_value_item_type(self) -> ItemType:
        return self.value_item_type

    def set_value_item_type(self, value_item_type: ItemType) -> Native:
        if value_item_type in (ItemType.Auto, None):
            self.value_item_type = ItemType.Any
        else:
            try:
                value_item_type = ItemType(value_item_type)
            except ValueError:
                value_item_type = ItemType(value_item_type.value)
            self.value_item_type = value_item_type or ItemType.Any
        return self

    def get_struct(self) -> FlatStruct:
        struct = FlatStruct(['key', 'value']).set_types(key='any', value='any')
        assert isinstance(struct, FlatStruct)
        return struct

    @deprecated_with_alternative('select()')
    def map_keys(self, func: Callable) -> Native:
        stream = self.map(lambda i: (func(KEY(i)), VALUE(i)))
        return self._assume_native(stream)

    @deprecated_with_alternative('select()')
    def map_values(self, func: Callable) -> Native:
        stream = self.map(lambda i: (KEY(i), func(VALUE(i))))
        return self._assume_native(stream)

    @deprecated_with_alternative('get_one_column_values()')
    def values(self) -> RegularStreamInterface:
        item_type = self.get_value_item_type()
        stream = self.map_to_type(VALUE, item_type=item_type)
        return self._assume_regular(stream)

    @deprecated_with_alternative('get_one_column_values()')
    def keys(self, uniq: bool, item_type: ItemType = ItemType.Auto) -> RegularStreamInterface:
        items = self.get_uniq_keys() if uniq else self._get_mapped_items(KEY)
        if item_type in (ItemType.Auto, None):
            item_type = ItemType.Any
        stream = self.stream(items, item_type=item_type)
        return self._assume_regular(stream)

    def get_uniq_values(self, column=VALUE) -> Iterable:
        uniq_values = list()
        for i in self.get_one_column_values(column, as_list=False):
            if i not in uniq_values:
                uniq_values.append(i)
                yield i

    def get_uniq_keys(self, as_list: bool = False) -> Union[list, Iterable]:
        keys = self.get_uniq_values(KEY)
        if as_list:
            return list(keys)
        else:
            return keys

    @deprecated_with_alternative('flat_map(fs.unfold_lists())')
    def ungroup_values(
            self,
            key_func: Callable = KEY,
            value_func: Callable = VALUE,
    ) -> Native:
        func = fs.unfold_lists(value_func, key_func=key_func, number_field=None, item_type=self.get_item_type())
        stream = self.flat_map(func)
        return self._assume_native(stream)

    @staticmethod
    def _assume_regular(stream) -> RegularStreamInterface:
        return stream

    @staticmethod
    def _assume_native(stream) -> Native:
        return stream
