from typing import Union, Iterable

try:  # Assume we're a submodule in a package.
    from utils.decorators import deprecated_with_alternative
    from streams.regular.regular_stream import RegularStream, Item, Count, Struct, Source, Context, TmpFiles, Auto, AUTO
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.decorators import deprecated_with_alternative
    from .regular_stream import RegularStream, Item, Count, Struct, Source, Context, TmpFiles, Auto, AUTO



class AnyStream(RegularStream):
    @deprecated_with_alternative('RegularStream')
    def __init__(
            self,
            data: Iterable[Item],
            name: Union[str, Auto] = AUTO,
            caption: str = '',
            count: Count = None,
            less_than: Count = None,
            struct: Struct = None,
            source: Source = None,
            context: Context = None,
            max_items_in_memory: Count = AUTO,
            tmp_files: TmpFiles = AUTO,
            check: bool = False,
    ):
        super().__init__(
            data=data, check=check,
            name=name, caption=caption,
            count=count, less_than=less_than,
            struct=struct,
            source=source, context=context,
            max_items_in_memory=max_items_in_memory,
            tmp_files=tmp_files,
        )
