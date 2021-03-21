from typing import Type, Union, Optional, Iterable
import pandas as pd

try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as sm
    from connectors import connector_classes as cs
    from functions import all_functions as fs
    from loggers.logger_classes import deprecated_with_alternative
    from selection import selection_classes as sn
    from items import base_item_type as it
    from utils import (
        arguments as arg,
        mappers as ms,
        selection,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import stream_classes as sm
    from ...connectors import connector_classes as cs
    from ...functions import all_functions as fs
    from ...loggers.logger_classes import deprecated_with_alternative
    from ...selection import selection_classes as sn
    from ...items import base_item_type as it
    from ...utils import (
        arguments as arg,
        mappers as ms,
        selection,
    )


def get_key_function(descriptions, take_hash=False):
    if len(descriptions) == 0:
        raise ValueError('key must be defined')
    elif len(descriptions) == 1:
        key_function = fs.partial(selection.value_from_record, descriptions[0])
    else:
        key_function = fs.partial(selection.tuple_from_record, descriptions)
    if take_hash:
        return lambda r: hash(key_function(r))
    else:
        return key_function


class RecordStream(sm.AnyStream, sm.ColumnarMixin, sm.ConvertMixin):
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
            data=data,
            name=name,
            count=count, less_than=less_than,
            source=source, context=context,
            max_items_in_memory=max_items_in_memory,
            tmp_files_template=tmp_files_template,
            tmp_files_encoding=tmp_files_encoding,
        )
        self.check = check

    @staticmethod
    def get_item_type() -> it.ItemType:
        return it.ItemType.Record

    def get_one_column(self, column, as_list=False):
        if as_list:
            return list(self.get_one_column(as_list=False))
        else:
            for r in self.get_records():
                yield r

    def get_columns(self, by_rows_count=100):
        if self.is_in_memory():
            examples = self.take(by_rows_count)
        else:
            examples = self.get_tee_stream().take(by_rows_count)
        columns = set()
        for r in examples.get_items():
            columns.update(r.keys())
        return columns

    def get_records(self, columns=arg.DEFAULT):
        if columns == arg.DEFAULT:
            return self.get_items()
        else:
            return self.select(*columns)

    def get_enumerated_records(self, field='#', first=1) -> Iterable:
        for n, r in enumerate(self.get_data()):
            r[field] = n + first
            yield r

    def enumerate(self, native=False):
        if native:
            return self.stream(
                self.get_enumerated_records(),
            )
        else:
            return self.stream(
                self.enumerated_items(),
                stream_type=sm.StreamType.KeyValueStream,
                secondary=self.get_stream_type(),
            )

    def select(self, *fields, use_extended_method=False, **expressions):
        return self.native_map(
            sn.select(
                *fields, **expressions,
                target_item_type=it.ItemType.Record, input_item_type=it.ItemType.Record,
                logger=self.get_logger(), selection_logger=self.get_selection_logger(),
                use_extended_method=use_extended_method,
            )
        )

    def filter(self, *fields, **expressions):
        expressions_list = [
            (k, fs.equal(v) if isinstance(v, (str, int, float, bool)) else v)
            for k, v in expressions.items()
        ]
        extended_filters_list = list(fields) + expressions_list

        def filter_function(r):
            for f in extended_filters_list:
                if not selection.value_from_record(r, f):
                    return False
            return True
        props = self.get_meta()
        props.pop('count')
        filtered_items = filter(filter_function, self.get_items())
        if self.is_in_memory():
            filtered_items = list(filtered_items)
            props['count'] = len(filtered_items)
        return self.__class__(
            filtered_items,
            **props
        )

    def sort(
            self,
            *keys,
            reverse=False,
            step=arg.DEFAULT,
            verbose=True,
    ):
        key_function = get_key_function(keys)
        step = arg.undefault(step, self.max_items_in_memory)
        if self.can_be_in_memory(step=step):
            return self.memory_sort(key_function, reverse, verbose=verbose)
        else:
            return self.disk_sort(key_function, reverse, step=step, verbose=verbose)

    def sorted_group_by(self, *keys, values=None, as_pairs=False):
        keys = arg.update(keys)

        def get_groups():
            key_function = get_key_function(keys)
            accumulated = list()
            prev_k = None
            for r in self.get_items():
                k = key_function(r)
                if (k != prev_k) and accumulated:
                    yield (prev_k, accumulated) if as_pairs else accumulated
                    accumulated = list()
                prev_k = k
                accumulated.append(r)
            yield (prev_k, accumulated) if as_pairs else accumulated
        if as_pairs:
            sm_groups = sm.KeyValueStream(get_groups(), value_stream_type=sm.StreamType.RowStream)
        else:
            sm_groups = sm.RowStream(get_groups(), check=False)
        if values:
            sm_groups = sm_groups.map_to_type(
                lambda r: ms.fold_lists(r, keys, values),
                stream_type=sm.StreamType.RecordStream,
            )
        if self.is_in_memory():
            return sm_groups.to_memory()
        else:
            sm_groups.set_estimated_count(self.get_count() or self.get_estimated_count())
            return sm_groups

    def group_by(self, *keys, values=None, step=arg.DEFAULT, as_pairs=False, take_hash=True, verbose=True):
        keys = arg.update(keys)
        step = arg.undefault(step, self.max_items_in_memory)
        if as_pairs:
            key_for_sort = keys
        else:
            key_for_sort = get_key_function(keys, take_hash=take_hash)
        sorted_stream = self.sort(
            key_for_sort,
            step=step,
            verbose=verbose,
        )
        grouped_stream = sorted_stream.sorted_group_by(
            keys,
            values=values,
            as_pairs=as_pairs,
        )
        return grouped_stream

    def get_dataframe(self, columns=None) -> Type[pd.DataFrame]:
        dataframe = pd.DataFrame(self.get_data())
        if columns:
            dataframe = dataframe[columns]
        return dataframe

    def to_line_stream(self, columns, add_title_row=False, delimiter='\t'):
        return sm.LineStream(
            self.to_row_stream(columns, add_title_row=add_title_row),
            count=self.count,
            less_than=self.less_than,
        ).map(
            delimiter.join,
        )

    def get_rows(self, columns=arg.DEFAULT, add_title_row=False):
        columns = arg.undefault(columns, self.get_columns())
        if add_title_row:
            yield columns
        for r in self.get_items():
            yield [r.get(c) for c in columns]

    def to_row_stream(self, *columns, **kwargs):
        add_title_row = kwargs.pop('add_title_row', None)
        columns = arg.update(columns, kwargs.pop('columns', None))
        if kwargs:
            raise ValueError('to_row_stream(): {} arguments are not supported'.format(kwargs.keys()))
        if self.get_count() is None:
            count = None
        else:
            count = self.count + (1 if add_title_row else 0)
        return sm.RowStream(
            self.get_rows(list(columns)),
            count=count,
            less_than=self.less_than,
        )

    def schematize(self, schema, skip_bad_rows=False, skip_bad_values=False, verbose=True):
        return self.to_row_stream(
            columns=schema.get_columns(),
        ).schematize(
            schema=schema,
            skip_bad_rows=skip_bad_rows,
            skip_bad_values=skip_bad_values,
            verbose=verbose,
        )

    def to_key_value_stream(self, key='key', value=None, skip_errors=False):
        kws = dict(logger=self.get_logger(), skip_errors=skip_errors)

        def get_pairs():
            for i in self.get_items():
                k = selection.value_from_record(i, key, **kws)
                v = i if value is None else selection.value_from_record(i, value, **kws)
                yield k, v
        return self.stream(
            list(get_pairs()) if self.is_in_memory() else get_pairs(),
            stream_type=sm.StreamType.KeyValueStream,
            value_stream_type=sm.StreamType.RecordStream if value is None else sm.StreamType.AnyStream,
            check=False,
        )

    def to_file(self, file, verbose=True, return_stream=True):
        assert cs.is_file(file), TypeError('Expected TsvFile, got {} as {}'.format(file, type(file)))
        meta = self.get_meta()
        if not file.gzip:
            meta.pop('count')
        file.write_stream(self, verbose=verbose)
        if return_stream:
            return file.to_record_stream(verbose=verbose).update_meta(**meta)

    @deprecated_with_alternative('RecordStream.to_file()')
    def to_tsv_file(self, *args, **kwargs):
        return self.to_file(*args, **kwargs)

    def to_column_file(
            self,
            filename,
            columns,
            delimiter='\t',
            add_title_row=True,
            encoding=arg.DEFAULT,
            gzip=False,
            check=True,
            verbose=True,
            return_stream=True,
    ):
        encoding = arg.undefault(encoding, self.tmp_files_encoding)
        meta = self.get_meta()
        if not gzip:
            meta.pop('count')
        sm_csv_file = self.to_row_stream(
            columns=columns,
            add_title_row=add_title_row,
        ).to_line_stream(
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
    def from_column_file(
            cls,
            filename,
            columns,
            delimiter='\t',
            skip_first_line=True,
            encoding=arg.DEFAULT,
            gzip=False,
            check=arg.DEFAULT,
            expected_count=arg.DEFAULT,
            verbose=True,
    ):
        encoding = arg.undefault(encoding, sm.TMP_FILES_ENCODING)
        return sm.LineStream.from_text_file(
            filename,
            skip_first_line=skip_first_line,
            encoding=encoding,
            gzip=gzip,
            check=check,
            expected_count=expected_count,
            verbose=verbose,
        ).to_row_stream(
            delimiter=delimiter,
        ).to_record_stream(
            columns=columns,
        )

    @classmethod
    def from_json_file(
            cls,
            filename,
            encoding=None,
            gzip=False,
            default_value=None,
            max_count=None,
            check=True,
            verbose=False,
    ):
        parsed_stream = sm.LineStream.from_text_file(
            filename,
            encoding=encoding,
            gzip=gzip,
            max_count=max_count,
            check=check,
            verbose=verbose,
        ).parse_json(
            default_value=default_value,
            to=sm.StreamType.RecordStream,
        )
        return parsed_stream

    def get_dict(self, key, value=None, of_lists=False, skip_errors=False):
        return self.to_key_value_stream(
            key, value,
            skip_errors=skip_errors,
        ).get_dict(
            of_lists,
        )

    def get_description(self):
        return '{} rows, {} columns: {}'.format(
            self.get_str_count(),
            self.get_column_count(),
            ', '.join(self.get_columns()),
        )
