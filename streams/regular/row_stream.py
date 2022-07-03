from typing import Iterable, Generator, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        Stream, RegularStream, StructStream, StructInterface, Struct,
        Context, Connector, TmpFiles, ItemType, StreamType,
        Name, Count, Columns, Array, ARRAY_TYPES,
        AUTO, Auto, AutoCount, AutoColumns,
    )
    from base.functions.arguments import get_names, update
    from utils.decorators import deprecated_with_alternative
    from functions.primary import numeric as nm
    from functions.secondary.array_functions import fold_lists
    from content.selection import selection_classes as sn, selection_functions as sf
    from streams.mixin.columnar_mixin import ColumnarMixin
    from streams.regular.any_stream import AnyStream
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        Stream, RegularStream, StructStream, StructInterface, Struct,
        Context, Connector, TmpFiles, ItemType, StreamType,
        Name, Count, Columns, Array, ARRAY_TYPES,
        AUTO, Auto, AutoCount, AutoColumns,
    )
    from ...base.functions.arguments import get_names, update
    from ...utils.decorators import deprecated_with_alternative
    from ...functions.primary import numeric as nm
    from ...functions.secondary.array_functions import fold_lists
    from ...content.selection import selection_classes as sn, selection_functions as sf
    from ..mixin.columnar_mixin import ColumnarMixin
    from .any_stream import AnyStream

Native = RegularStream

DEFAULT_EXAMPLE_COUNT = 10


class RowStream(AnyStream, ColumnarMixin):
    def __init__(
            self,
            data: Iterable,
            name: Name = AUTO,
            caption: str = '',
            count: Count = None,
            less_than: Count = None,
            struct: Struct = None,
            source: Connector = None,
            context: Context = None,
            max_items_in_memory: AutoCount = AUTO,
            tmp_files: Union[TmpFiles, Auto] = AUTO,
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
    def get_item_type() -> ItemType:
        return ItemType.Row

    def to_line_stream(self, delimiter: str = '\t', columns: AutoColumns = AUTO, add_title_row=False) -> Stream:
        input_stream = self.select(columns) if Auto.is_defined(columns) else self
        lines = map(lambda r: '\t'.join([str(c) for c in r]), input_stream.get_items())
        line_stream_class = StreamType.LineStream.get_class()
        return line_stream_class(lines, count=self.get_count())

    def get_records(self, columns: AutoColumns = AUTO) -> Generator:
        if columns == AUTO:
            columns = self.get_columns()
        column_names = get_names(columns)
        for row in self.get_rows():
            yield {k: v for k, v in zip(column_names, row)}

    def structure(
            self,
            struct: StructInterface,
            skip_bad_rows: bool = False,
            skip_bad_values: bool = False,
            verbose: bool = True,
    ) -> StructStream:
        struct_stream_class = StreamType.StructStream.get_class()
        stream = struct_stream_class([], struct=struct, **self.get_meta(ex='struct'))
        data = stream.get_struct_rows(self.get_items())
        stream = stream.add_items(data)
        if self.is_in_memory():
            stream = stream.collect()
        stream.set_struct(struct, inplace=True)
        return stream

    @classmethod
    @deprecated_with_alternative('connectors.ColumnFile()')
    def from_column_file(
            cls,
            filename,
            delimiter='\t',
            skip_first_line=False, max_count=None,
            check=AUTO,
            verbose=False,
    ):
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

    @deprecated_with_alternative('to_file(Connectors.ColumnFile)')
    def to_column_file(
            self,
            filename,
            delimiter='\t',
            check=AUTO,
            verbose=True,
            return_stream=True,
    ):
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
