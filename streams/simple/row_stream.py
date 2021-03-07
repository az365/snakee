try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as sm
    from utils import (
        arguments as arg,
        items as it,
        numeric as nm,
        selection as sf,
    )
    from selection import selection_classes as sn
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import stream_classes as sm
    from ...utils import (
        arguments as arg,
        items as it,
        numeric as nm,
        selection as sf,
    )
    from ...selection import selection_classes as sn


def is_row(item):
    return it.ItemType.Row.isinstance(item)


def check_rows(rows, skip_errors=False):
    for i in rows:
        if is_row(i):
            pass
        elif skip_errors:
            continue
        else:
            raise TypeError('check_rows(): this item is not row: {}'.format(i))
        yield i


class RowStream(sm.AnyStream, sm.ColumnarStream):
    def __init__(
            self,
            data,
            name=arg.DEFAULT,
            count=None,
            less_than=None,
            check=True,
            source=None,
            max_items_in_memory=sm.MAX_ITEMS_IN_MEMORY,
            tmp_files_template=sm.TMP_FILES_TEMPLATE,
            tmp_files_encoding=sm.TMP_FILES_ENCODING,
            context=None,
    ):
        super().__init__(
            check_rows(data) if check else data,
            name=name,
            count=count,
            less_than=less_than,
            source=source,
            context=context,
            max_items_in_memory=max_items_in_memory,
            tmp_files_template=tmp_files_template,
            tmp_files_encoding=tmp_files_encoding,
        )
        self.check = check

    @staticmethod
    def get_item_type():
        return it.ItemType.Row

    @staticmethod
    def is_valid_item(item):
        return is_row(item)

    @staticmethod
    def get_valid_items(items, skip_errors=False):
        return check_rows(items, skip_errors)

    def get_column_count(self, take=10, get_max=True, get_min=False):
        if self.is_in_memory() and (get_max or get_min):
            example_stream = self.take(take)
        else:
            example_stream = self.get_tee_stream().take(take)
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

    def get_columns(self, **kwargs):
        count = self.get_column_count(**kwargs)
        return list(range(count))

    def get_one_column(self, column):
        return self.select([column])

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

    def group_by(self, *keys):
        items = self.select(keys, lambda r: r).get_items()
        return sm.KeyValueStream(items, **self.get_meta())

    def get_dataframe(self, columns=None):
        if columns:
            return nm.pd.DataFrame(self.get_data(), columns=columns)
        else:
            return nm.pd.DataFrame(self.get_data())

    def to_lines(self, delimiter='\t'):
        return sm.LineStream(
            map(lambda r: '\t'.join([str(c) for c in r]), self.get_items()),
            count=self.count,
        )

    def to_records(self, function=None, columns=tuple()):
        if function:
            records = map(function, self.get_items())
        elif columns:
            records = self.get_records(columns=columns)
        else:
            records = map(lambda r: dict(row=r), self.get_items())
        return sm.RecordStream(
            records,
            **self.get_meta()
        )

    def get_records(self, columns=arg.DEFAULT):
        # columns = arg.undefault(columns, self.get_columns())
        if columns == arg.DEFAULT:
            columns = self.get_columns()
        for row in self.get_rows():
            yield {k: v for k, v in zip(columns, row)}

    def get_rows(self):
        return self.get_data()

    def schematize(self, schema, skip_bad_rows=False, skip_bad_values=False, verbose=True):
        return sm.SchemaStream(
            self.get_items(),
            **self.get_meta(),
        ).schematize(
            schema=schema,
            skip_bad_rows=skip_bad_rows,
            skip_bad_values=skip_bad_values,
            verbose=verbose,
        )

    @classmethod
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
        ).to_rows(
            delimiter=delimiter
        )
        return fx_rows

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
        fx_csv_file = self.to_lines(
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
            return fx_csv_file.to_rows(
                delimiter=delimiter,
            ).update_meta(
                **meta
            )
