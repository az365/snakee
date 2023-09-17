from typing import Optional, Iterable

try:  # Assume we're a submodule in a package.
    from interfaces import Struct, ItemType, Name, Count, Source, Context, TmpFiles
    from streams.mixin.convert_mixin import ConvertMixin
    from streams.regular.row_stream import RowStream, deprecated_with_alternative
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import Struct, ItemType, Name, Count, Source, Context, TmpFiles
    from ..mixin.convert_mixin import ConvertMixin
    from .row_stream import RowStream, deprecated_with_alternative

EXPECTED_ITEM_TYPE = ItemType.Row


class StructStream(RowStream, ConvertMixin):
    @deprecated_with_alternative('RegularStream(item_type=ItemType.StructRow)')
    def __init__(
            self,
            data: Iterable,
            name: Optional[Name] = None,
            caption: str = '',
            item_type: ItemType = EXPECTED_ITEM_TYPE,
            struct: Struct = None,
            source: Source = None,
            context: Context = None,
            count: Count = None,
            less_than: Count = None,
            max_items_in_memory: Count = None,
            tmp_files: TmpFiles = None,
            check: bool = True,
    ):
        super().__init__(
            data=data, check=check,
            name=name, caption=caption,
            item_type=item_type, struct=struct,
            source=source, context=context,
            count=count, less_than=less_than,
            max_items_in_memory=max_items_in_memory,
            tmp_files=tmp_files,
        )

    @staticmethod
    def get_default_item_type() -> ItemType:
        return EXPECTED_ITEM_TYPE
