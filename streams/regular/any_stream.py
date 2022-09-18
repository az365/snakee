from typing import Union, Iterable

try:  # Assume we're a submodule in a package.
    from utils.decorators import deprecated_with_alternative
    from streams.regular.regular_stream import (
        RegularStream, Item, ItemType,
        Count, Struct, Source, Context, TmpFiles, Auto, AUTO,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.decorators import deprecated_with_alternative
    from .regular_stream import (
        RegularStream, Item, ItemType,
        Count, Struct, Source, Context, TmpFiles, Auto, AUTO,
    )

EXPECTED_ITEM_TYPE = ItemType.Any


class AnyStream(RegularStream):
    @deprecated_with_alternative('RegularStream')
    def __init__(
            self,
            data: Iterable[Item],
            name: Union[str, Auto] = AUTO,
            caption: str = '',
            item_type: ItemType = ItemType.Any,
            struct: Struct = None,
            count: Count = None,
            less_than: Count = None,
            source: Source = None,
            context: Context = None,
            max_items_in_memory: Count = AUTO,
            tmp_files: TmpFiles = AUTO,
            check: bool = False,
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

    def _set_item_type_inplace(self, item_type: ItemType) -> None:
        if item_type != self.get_default_item_type():
            raise TypeError(f'Can not set item_type={item_type} for deprecated class LineStream')
