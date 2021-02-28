try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as sm
    from utils import (
        arguments as arg,
        items as it,
        selection,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import stream_classes as sm
    from ...utils import (
        arguments as arg,
        items as it,
        selection,
    )


def is_row(row):
    return isinstance(row, (list, tuple))


def check_rows(rows, skip_errors=False):
    for i in rows:
        if is_row(i):
            pass
        elif skip_errors:
            continue
        else:
            raise TypeError('check_rows(): this item is not row: {}'.format(i))
        yield i


class RowStream(sm.AnyStream):
    def __init__(
            self,
            data,
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
    def is_valid_item(item):
        return is_row(item)

    @staticmethod
    def valid_items(items, skip_errors=False):
        return check_rows(items, skip_errors)

    def select(self, *columns):
        return self.native_map(
            selection.select(
                *columns,
                target_item_type=it.ItemType.Row, input_item_type=it.ItemType.Row,
                logger=self.get_logger(), selection_logger=self.get_selection_logger(),
            )
        )

    def to_records(self, function=None, columns=tuple()):
        def get_records(rows, cols):
            for r in rows:
                yield {k: v for k, v in zip(cols, r)}
        if function:
            records = map(function, self.get_items())
        elif columns:
            records = get_records(self.get_items(), columns)
        else:
            records = map(lambda r: dict(row=r), self.get_items())
        return sm.RecordStream(
            records,
            **self.get_meta()
        )

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

    def to_lines(self, delimiter='\t'):
        return sm.LineStream(
            map(lambda r: '\t'.join([str(c) for c in r]), self.get_items()),
            count=self.count,
        )
