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

    def select(self, *columns, use_extended_method: bool = True) -> Native:
        select_function = sn.get_selection_function(
            *columns,
            use_extended_method=use_extended_method,
            target_item_type=ItemType.Row, input_item_type=ItemType.Row,
            logger=self.get_logger(), selection_logger=self.get_selection_logger(),
        )
        return self.native_map(select_function)

    def sorted_group_by(
            self,
            *keys,
            values: Columns = None,
            skip_missing: bool = True,  # tmp
            as_pairs: bool = False,
            output_struct: Struct = None,
            take_hash: bool = False,
    ) -> Stream:
        keys = update(keys)
        key_function = self._get_key_function(keys, take_hash=take_hash)
        iter_groups = self._get_groups(key_function, as_pairs=as_pairs)
        if as_pairs:
            stream_builder = StreamType.KeyValueStream.get_class()
            stream_groups = stream_builder(iter_groups, value_stream_type=self.get_stream_type())
        else:
            stream_builder = StreamType.RowStream.get_class()
            stream_groups = stream_builder(iter_groups, check=False)
        if values:
            stream_type = self.get_stream_type()
            item_type = self.get_item_type()
            if item_type == ItemType.Row or stream_type == StreamType.RowStream:
                keys = [self._get_field_getter(f) for f in keys]
                values = [self._get_field_getter(f, item_type=item_type) for f in values]
            fold_mapper = fold_lists(keys=keys, values=values, skip_missing=skip_missing, item_type=item_type)
            stream_groups = stream_groups.map_to_type(fold_mapper, stream_type=stream_type)
            if output_struct:
                if hasattr(stream_groups, 'structure'):
                    stream_groups = stream_groups.structure(output_struct)
                else:
                    stream_groups.set_struct(output_struct, check=False, inplace=True)
        if self.is_in_memory():
            return stream_groups.to_memory()
        else:
            stream_groups.set_estimated_count(self.get_count() or self.get_estimated_count(), inplace=True)
            return stream_groups

    def get_dataframe(self, columns: Columns = None):
        if columns:
            return nm.DataFrame(self.get_data(), columns=columns)
        else:
            return nm.DataFrame(self.get_data())

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
