from typing import Optional, Iterable

try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as sm
    from utils import (
        arguments as arg,
        items as it,
        numeric as nm,
        selection as sf,
    )
    from selection import selection_classes as sn
    from loggers.logger_classes import deprecated_with_alternative
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import stream_classes as sm
    from ...utils import (
        arguments as arg,
        items as it,
        numeric as nm,
        selection as sf,
    )
    from ...selection import selection_classes as sn
    from ...loggers.logger_classes import deprecated_with_alternative

Stream = sm.StreamInterface
Native = sm.RegularStreamInterface


class RowStream(sm.AnyStream, sm.ColumnarMixin):
    def __init__(
            self,
            data,
            name=arg.DEFAULT, check=True,
            count=None, less_than=None,
            source=None, context=None,
            max_items_in_memory=sm.MAX_ITEMS_IN_MEMORY,
            tmp_files_template=sm.TMP_FILES_TEMPLATE,
            tmp_files_encoding=sm.TMP_FILES_ENCODING,
    ):
        super().__init__(
            data,
            name=name, check=check,
            count=count, less_than=less_than,
            source=source, context=context,
            max_items_in_memory=max_items_in_memory,
            tmp_files_template=tmp_files_template,
            tmp_files_encoding=tmp_files_encoding,
        )
        self.check = check

    @staticmethod
    def get_item_type():
        return it.ItemType.Row

    def get_column_count(self, take=10, get_max=True, get_min=False) -> int:
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

    def select(self, *columns, use_extended_method=True):
        if use_extended_method:
            selection_method = sn.select
        else:
            selection_method = sf.select
        select_function = selection_method(
            *columns,
            target_item_type=it.ItemType.Row, input_item_type=it.ItemType.Row,
            logger=self.get_logger(), selection_logger=self.get_selection_logger(),
        )
        return self.native_map(
            select_function,
        )

    def get_dataframe(self, columns=None):
        if columns:
            return nm.pd.DataFrame(self.get_data(), columns=columns)
        else:
            return nm.pd.DataFrame(self.get_data())

    def to_line_stream(self, delimiter='\t') -> Stream:
        return sm.LineStream(
            map(lambda r: '\t'.join([str(c) for c in r]), self.get_items()),
            count=self.get_count(),
        )

    def get_records(self, columns=arg.DEFAULT):
        if columns == arg.DEFAULT:
            columns = self.get_columns()
        for row in self.get_rows():
            yield {k: v for k, v in zip(columns, row)}

    def get_rows(self, **kwargs):
        return self.get_data()

    def schematize(self, schema, skip_bad_rows=False, skip_bad_values=False, verbose=True) -> Stream:
        result = sm.SchemaStream(
            self.get_items(),
            **self.get_compatible_meta(sm.StreamType.SchemaStream),
        ).schematize(
            schema=schema,
            skip_bad_rows=skip_bad_rows,
            skip_bad_values=skip_bad_values,
            verbose=verbose,
        )
        return result

    @classmethod
    # @deprecated_with_alternative('connectors.ColumnFile()')
    def from_column_file(
            cls,
            filename,
            encoding=None, gzip=False,
            delimiter='\t',
            skip_first_line=False, max_count=None,
            check=arg.DEFAULT,
            verbose=False,
    ):
        fx_rows = sm.LineStream.from_text_file(
            filename,
            encoding=encoding, gzip=gzip,
            skip_first_line=skip_first_line, max_count=max_count,
            check=check,
            verbose=verbose,
        ).to_row_stream(
            delimiter=delimiter
        )
        return fx_rows

    @deprecated_with_alternative('to_file(Connectors.ColumnFile)')
    def to_column_file(
            self,
            filename,
            delimiter='\t',
            encoding=arg.DEFAULT,
            gzip=False,
            check=arg.DEFAULT,
            verbose=True,
            return_stream=True,
    ):
        encoding = arg.undefault(encoding, self.tmp_files_encoding)
        meta = self.get_meta()
        if not gzip:
            meta.pop('count')
        fx_csv_file = self.to_line_stream(
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
            return fx_csv_file.to_row_stream(
                delimiter=delimiter,
            ).update_meta(
                **meta
            )
