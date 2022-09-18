from typing import Optional, Iterable, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        Struct, Connector, Context, TmpFiles, ItemType, StreamType,
        AUTO, Auto, AutoBool, AutoCount, Count, Name,
    )
    from base.constants.chars import EMPTY, TAB_CHAR
    from utils.decorators import deprecated_with_alternative
    from streams.mixin.columnar_mixin import ColumnarMixin
    from streams.regular.any_stream import AnyStream
    from streams.regular.line_stream import LineStream
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        Struct, Connector, Context, TmpFiles, ItemType, StreamType,
        AUTO, Auto, AutoBool, AutoCount, Count, Name,
    )
    from ...base.constants.chars import EMPTY, TAB_CHAR
    from ...utils.decorators import deprecated_with_alternative
    from ..mixin.columnar_mixin import ColumnarMixin
    from .any_stream import AnyStream
    from .line_stream import LineStream

EXPECTED_ITEM_TYPE = ItemType.Row


class RowStream(AnyStream, ColumnarMixin):
    @deprecated_with_alternative('RegularStream(item_type=ItemType.Row)')
    def __init__(
            self,
            data: Iterable,
            name: Name = AUTO,
            caption: str = EMPTY,
            item_type: ItemType = EXPECTED_ITEM_TYPE,
            struct: Struct = None,
            source: Connector = None,
            context: Context = None,
            count: Count = None,
            less_than: Count = None,
            max_items_in_memory: AutoCount = AUTO,
            tmp_files: Union[TmpFiles, Auto] = AUTO,
            check: bool = False,
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

    @staticmethod
    def get_default_item_type() -> ItemType:
        return EXPECTED_ITEM_TYPE

    @classmethod
    @deprecated_with_alternative('connectors.ColumnFile().to_stream()')
    def from_column_file(
            cls,
            filename: str,
            delimiter: str = TAB_CHAR,
            skip_first_line: bool = False,
            max_count: Count = None,
            check: AutoBool = AUTO,
            verbose: bool = False,
    ) -> AnyStream:
        stream = LineStream.from_text_file(
            filename,
            skip_first_line=skip_first_line, max_count=max_count,
            check=check,
            verbose=verbose,
        ).to_row_stream(
            delimiter=delimiter
        )
        return stream

    @deprecated_with_alternative('RegularStream.to_file(Connectors.ColumnFile)')
    def to_column_file(
            self,
            filename: str,
            delimiter: str = TAB_CHAR,
            check: AutoBool = AUTO,
            verbose: bool = True,
            return_stream: bool = True,
    ) -> Optional[AnyStream]:
        meta = self.get_meta()
        stream_csv_file = self.to_line_stream(
            delimiter=delimiter,
        ).to_text_file(
            filename,
            check=check,
            verbose=verbose,
            return_stream=return_stream,
        )
        if return_stream:
            return stream_csv_file.to_row_stream(
                delimiter=delimiter,
            ).update_meta(
                **meta
            )
