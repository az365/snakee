from typing import Iterable, Union

try:  # Assume we're a submodule in a package.
    from interfaces import Struct, ItemType, Name, Count, Source, Context, TmpFiles, Auto, AUTO
    from content.struct.struct_mixin import StructMixin
    from streams.mixin.convert_mixin import ConvertMixin
    from streams.regular.row_stream import RowStream
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import Struct, ItemType, Name, Count, Source, Context, TmpFiles, Auto, AUTO
    from ...content.struct.struct_mixin import StructMixin
    from ..mixin.convert_mixin import ConvertMixin
    from .row_stream import RowStream

EXPECTED_ITEM_TYPE = ItemType.StructRow


class StructStream(RowStream, StructMixin, ConvertMixin):
    def __init__(
            self,
            data: Iterable,
            struct: Struct = None,
            name: Union[Name, Auto] = AUTO,
            caption: str = '',
            count: Count = None,
            less_than: Count = None,
            source: Source = None,
            context: Context = None,
            max_items_in_memory: Count = AUTO,
            tmp_files: TmpFiles = AUTO,
            check: bool = True,
    ):
        super().__init__(
            data=data, struct=struct, check=check,
            name=name, caption=caption,
            count=count, less_than=less_than,
            source=source, context=context,
            max_items_in_memory=max_items_in_memory,
            tmp_files=tmp_files,
        )

    @staticmethod
    def get_default_item_type() -> ItemType:
        return EXPECTED_ITEM_TYPE
