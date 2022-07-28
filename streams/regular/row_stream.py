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
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        Struct, Connector, Context, TmpFiles, ItemType, StreamType,
        AUTO, Auto, AutoBool, AutoCount, Count, Name,
    )
    from ...base.constants.chars import EMPTY, TAB_CHAR
    from ...utils.decorators import deprecated_with_alternative
    from ..mixin.columnar_mixin import ColumnarMixin
    from .any_stream import AnyStream

EXPECTED_ITEM_TYPE = ItemType.Row


class RowStream(AnyStream, ColumnarMixin):
    def __init__(
            self,
            data: Iterable,
            name: Name = AUTO,
            caption: str = EMPTY,
            count: Count = None,
            less_than: Count = None,
            struct: Struct = None,
            source: Connector = None,
            context: Context = None,
            max_items_in_memory: AutoCount = AUTO,
            tmp_files: Union[TmpFiles, Auto] = AUTO,
            check: bool = False,
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
        line_stream_class = StreamType.LineStream.get_class()
        stream = line_stream_class.from_text_file(
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
