from typing import Optional, Callable, Iterable, Generator, Union

try:  # Assume we're a sub-module in a package.
    from interfaces import (
        Stream, RegularStream, StructStream, StructInterface,
        Context, Connector, TmpFiles, ItemType, StreamType,
        Name, Count, Columns,
        AUTO, Auto, AutoCount, AutoColumns,
    )
    from utils import arguments as arg, selection as sf
    from utils.decorators import deprecated_with_alternative
    from functions.primary import numeric as nm
    from selection import selection_classes as sn
    from streams.mixin.columnar_mixin import ColumnarMixin
    from streams.regular.any_stream import AnyStream
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        Stream, RegularStream, StructStream, StructInterface,
        Context, Connector, TmpFiles, ItemType, StreamType,
        Name, Count, Columns,
        AUTO, Auto, AutoCount, AutoColumns,
    )
    from ...utils import arguments as arg, selection as sf
    from ...utils.decorators import deprecated_with_alternative
    from ...functions.primary import numeric as nm
    from ...selection import selection_classes as sn
    from ..mixin.columnar_mixin import ColumnarMixin
    from .any_stream import AnyStream

Native = RegularStream

DEFAULT_EXAMPLE_COUNT = 10


class RowStream(AnyStream, ColumnarMixin):
    def __init__(
            self,
            data: Iterable,
            name: Name = AUTO, check: bool = True,
            count: Count = None, less_than: Count = None,
            source: Connector = None, context: Context = None,
            max_items_in_memory: AutoCount = AUTO,
            tmp_files: Union[TmpFiles, Auto] = AUTO,
    ):
        super().__init__(
            data,
            name=name, check=check,
            count=count, less_than=less_than,
            source=source, context=context,
            max_items_in_memory=max_items_in_memory,
            tmp_files=tmp_files,
        )

    @staticmethod
    def get_item_type() -> ItemType:
        return ItemType.Row

    def get_column_count(self, take: Count = DEFAULT_EXAMPLE_COUNT, get_max: bool = True, get_min: bool = False) -> int:
        if self.is_in_memory() and (get_max or get_min):
            example_stream = self.take(take)
        else:
            example_stream = self.tee_stream().take(take)
        count = 0
        for row in example_stream.get_items():
            row_len = len(row)
            if get_max:
                if row_len > count:
                    count = row_len
            else:  # elif get_min:
                if row_len < count:
                    count = row_len
        return count

    def get_columns(self, **kwargs) -> list:
        count = self.get_column_count(**kwargs)
        return list(range(count))

    def get_one_column_values(self, column) -> Iterable:
        return self.select([column]).get_items()

    @staticmethod
    def _get_selection_method(extended: bool = True) -> Callable:
        if extended:
            return sn.select
        else:
            return sf.select

    def select(self, *columns, use_extended_method: bool = True) -> Native:
        selection_method = self._get_selection_method(extended=use_extended_method)
        select_function = selection_method(
            *columns,
            target_item_type=ItemType.Row, input_item_type=ItemType.Row,
            logger=self.get_logger(), selection_logger=self.get_selection_logger(),
        )
        return self.native_map(select_function)

    def sorted_group_by(self, *keys, values: Optional[Iterable] = None, as_pairs: bool = False) -> Stream:
        raise NotImplementedError

    def group_by(self, *keys, values: Optional[Iterable] = None, as_pairs: bool = False) -> Stream:
        return self.sort(*keys).sorted_group_by(*keys, values=values, as_pairs=as_pairs)

    def get_dataframe(self, columns: Columns = None):
        if columns:
            return nm.DataFrame(self.get_data(), columns=columns)
        else:
            return nm.DataFrame(self.get_data())

    def to_line_stream(self, delimiter: str = '\t', columns: AutoColumns = AUTO, add_title_row=False) -> Stream:
        input_stream = self.select(columns) if arg.is_defined(columns) else self
        lines = map(lambda r: '\t'.join([str(c) for c in r]), input_stream.get_items())
        line_stream_class = StreamType.LineStream.get_class()
        return line_stream_class(lines, count=self.get_count())

    def get_records(self, columns: AutoColumns = AUTO) -> Generator:
        if columns == AUTO:
            columns = self.get_columns()
        column_names = arg.get_names(columns)
        for row in self.get_rows():
            yield {k: v for k, v in zip(column_names, row)}

    def get_rows(self, **kwargs) -> Iterable:
        return self.get_data()

    def structure(
            self,
            struct: StructInterface,
            skip_bad_rows: bool = False,
            skip_bad_values: bool = False,
            verbose: bool = True,
    ) -> StructStream:
        struct_stream_class = StreamType.StructStream.get_class()
        stream = struct_stream_class([], struct, **self.get_meta())
        data = stream.get_struct_rows(self.get_items())
        stream = stream.add_items(data)
        if self.is_in_memory():
            stream = stream.collect()
        return stream

    @classmethod
    @deprecated_with_alternative('connectors.ColumnFile()')
    def from_column_file(
            cls,
            filename,
            encoding=None, gzip=False,
            delimiter='\t',
            skip_first_line=False, max_count=None,
            check=AUTO,
            verbose=False,
    ):
        line_stream_class = StreamType.LineStream.get_class()
        stream = line_stream_class.from_text_file(
            filename,
            encoding=encoding, gzip=gzip,
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
            encoding=AUTO,
            gzip=False,
            check=AUTO,
            verbose=True,
            return_stream=True,
    ):
        encoding = arg.acquire(encoding, self.get_encoding())
        meta = self.get_meta()
        if not gzip:
            meta.pop('count')
        stream_csv_file = self.to_line_stream(
            delimiter=delimiter,
        ).to_text_file(
            filename,
            encoding=encoding,
            gzip=gzip,
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
