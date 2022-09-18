from typing import Optional, Iterable, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        RegularStreamInterface, Struct, Context, Connector, TmpFiles,
        ItemType, StreamType,
        AUTO, Auto, AutoName, AutoCount, Count,
    )
    from utils.decorators import deprecated_with_alternative
    from streams.mixin.convert_mixin import ConvertMixin
    from streams.mixin.columnar_mixin import ColumnarMixin
    from streams.regular.any_stream import AnyStream
    from streams.regular.line_stream import LineStream
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        RegularStreamInterface, Struct, Context, Connector, TmpFiles,
        ItemType, StreamType,
        AUTO, Auto, AutoName, AutoCount, Count,
    )
    from ...utils.decorators import deprecated_with_alternative
    from ..mixin.convert_mixin import ConvertMixin
    from ..mixin.columnar_mixin import ColumnarMixin
    from .any_stream import AnyStream
    from .line_stream import LineStream

EXPECTED_ITEM_TYPE = ItemType.Record


class RecordStream(AnyStream, ColumnarMixin, ConvertMixin):
    @deprecated_with_alternative('RegularStream(item_type=ItemType.Record)')
    def __init__(
            self,
            data: Iterable,
            name: AutoName = AUTO,
            caption: str = '',
            item_type: ItemType = EXPECTED_ITEM_TYPE,
            struct: Struct = None,
            source: Connector = None,
            context: Context = None,
            count: Count = None,
            less_than: Count = None,
            max_items_in_memory: AutoCount = AUTO,
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

    @deprecated_with_alternative('RegularStream.write_to()')
    def to_column_file(
            self, filename: str, columns: Union[Iterable, Auto] = AUTO,
            add_title_row=True, delimiter='\t',
            check=True, verbose=True, return_stream=True,
    ) -> Optional[RegularStreamInterface]:
        meta = self.get_meta()
        columns = Auto.delayed_acquire(columns, self.get_columns)
        row_stream = self.to_row_stream(columns=columns)
        if add_title_row:
            assert Auto.is_defined(columns)
            row_stream.add_items([columns], before=True, inplace=True)
        line_stream = row_stream.to_line_stream(delimiter=delimiter)
        sm_csv_file = line_stream.to_text_file(
            filename,
            check=check,
            verbose=verbose,
            return_stream=return_stream,
        )
        if return_stream:
            return sm_csv_file.skip(
                1 if add_title_row else 0,
            ).to_row_stream(
                delimiter=delimiter,
            ).to_record_stream(
                columns=columns,
            ).update_meta(
                **meta
            )

    @classmethod
    @deprecated_with_alternative('connectors.ColumnFile().to_stream()')
    def from_column_file(
            cls,
            filename, columns, delimiter='\t',
            skip_first_line=True, check=AUTO,
            expected_count=AUTO, verbose=True,
    ) -> RegularStreamInterface:
        return LineStream.from_text_file(
            filename, skip_first_line=skip_first_line,
            check=check, expected_count=expected_count, verbose=verbose,
        ).to_row_stream(
            delimiter=delimiter,
        ).to_record_stream(
            columns=columns,
        )
