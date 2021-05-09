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
        selection as sf,
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
        selection as sf,
    )

Stream = sm.StreamInterface
Native = sm.RegularStreamInterface
Pairs = sm.PairStreamInterface


def get_key_function(descriptions, take_hash=False):
    if len(descriptions) == 0:
        raise ValueError('key must be defined')
    elif len(descriptions) == 1:
        key_function = fs.partial(sf.value_from_record, descriptions[0])
    else:
        key_function = fs.partial(sf.tuple_from_record, descriptions)
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
            tmp_files=arg.DEFAULT,
    ):
        super().__init__(
            data=data,
            name=name,
            count=count, less_than=less_than,
            source=source, context=context,
            max_items_in_memory=max_items_in_memory,
            tmp_files=tmp_files,
        )
        self.check = check

    @staticmethod
    def get_item_type() -> it.ItemType:
        return it.ItemType.Record

    def get_one_column_values(self, column, as_list=False) -> Iterable:
        if as_list:
            return list(self.get_one_column_values(column, as_list=False))
        else:
            for r in self.get_records():
                yield r.get(column)

    def get_columns(self, by_rows_count=100) -> Iterable:
        if self.is_in_memory():
            examples = self.take(by_rows_count)
        else:
            examples = self.tee_stream().take(by_rows_count)
        columns = set()
        for r in examples.get_items():
            columns.update(r.keys())
        return columns

    def get_records(self, columns=arg.DEFAULT) -> Iterable:
        if columns == arg.DEFAULT:
            return self.get_items()
        else:
            return self.select(*columns).get_items()

    def get_enumerated_records(self, field='#', first=1) -> Iterable:
        for n, r in enumerate(self.get_data()):
            r[field] = n + first
            yield r

    def enumerate(self, native=False) -> Stream:
        if native:
            return self.stream(
                self.get_enumerated_records(),
            )
        else:
            return self.stream(
                self.get_enumerated_items(),
                stream_type=sm.StreamType.KeyValueStream,
                secondary=self.get_stream_type(),
            )

    def select(self, *fields, use_extended_method=False, **expressions) -> Native:
        stream = self.map(
            sn.select(
                *fields, **expressions,
                target_item_type=it.ItemType.Record, input_item_type=it.ItemType.Record,
                logger=self.get_logger(), selection_logger=self.get_selection_logger(),
                use_extended_method=use_extended_method,
            )
        )
        return self._assume_native(stream)

    def filter(self, *fields, **expressions) -> Native:
        filter_function = sf.filter_items(*fields, **expressions, item_type=it.ItemType.Record, skip_errors=True)
        filtered_items = self.get_filtered_items(filter_function)
        if self.is_in_memory():
            filtered_items = list(filtered_items)
            count = len(filtered_items)
            stream = self.stream(
                filtered_items,
                count=count,
                less_than=count,
            )
        else:
            stream = self.stream(
                filtered_items,
                count=None,
                less_than=self.get_estimated_count(),
            )
        return stream

    def sort(self, *keys, reverse=False, step=arg.DEFAULT, verbose=True) -> Native:
        key_function = get_key_function(keys)
        step = arg.undefault(step, self.max_items_in_memory)
        if self.can_be_in_memory(step=step):
            stream = self.memory_sort(key_function, reverse, verbose=verbose)
        else:
            stream = self.disk_sort(key_function, reverse, step=step, verbose=verbose)
        return self._assume_native(stream)

    def sorted_group_by(self, *keys, values=None, as_pairs=False) -> Stream:
        keys = arg.update(keys)
        keys = arg.get_names(keys)
        values = arg.get_names(values)

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

    def group_by(self, *keys, values=None, step=arg.DEFAULT, as_pairs=False, take_hash=True, verbose=True) -> Stream:
        keys = arg.update(keys)
        keys = arg.get_names(keys)
        values = arg.get_names(values)
        if hasattr(keys[0], 'get_field_names'):  # if isinstance(keys[0], FieldGroup)
            keys = keys[0].get_field_names()
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

    def group_to_pairs(self, *keys, values=None, step=arg.DEFAULT, verbose=True) -> Pairs:
        grouped_stream = self.group_by(*keys, values=values, step=step, as_pairs=True, take_hash=False, verbose=verbose)
        return self._assume_pairs(grouped_stream)

    def get_dataframe(self, columns=None) -> Type[pd.DataFrame]:
        dataframe = pd.DataFrame(self.get_data())
        if columns:
            dataframe = dataframe[columns]
        return dataframe

    def get_demo_example(self, count=10, filters: Optional[Iterable] = None, columns=None) -> Type[pd.DataFrame]:
        sm_sample = self.filter(*filters) if filters else self
        return sm_sample.take(count).get_dataframe(columns)

    def get_rows(self, columns: Union[Iterable, arg.DefaultArgument] = arg.DEFAULT, add_title_row=False) -> Iterable:
        columns = list(arg.delayed_undefault(columns, self.get_columns))
        if add_title_row:
            yield columns
        for r in self.get_items():
            yield [r.get(c) for c in columns]

    def to_row_stream(self, *columns, **kwargs) -> Stream:
        add_title_row = kwargs.pop('add_title_row', None)
        columns = arg.update(columns, kwargs.pop('columns', None))
        if kwargs:
            raise ValueError('to_row_stream(): {} arguments are not supported'.format(kwargs.keys()))
        count = self.get_count()
        less_than = self.get_estimated_count()
        if add_title_row:
            if count:
                count += 1
            if less_than:
                less_than += 1
        return self.stream(
            self.get_rows(columns=columns, add_title_row=add_title_row),
            stream_type=sm.StreamType.RowStream,
            count=count,
            less_than=less_than,
        )

    def schematize(self, schema, skip_bad_rows=False, skip_bad_values=False, verbose=True) -> Stream:
        return self.to_row_stream(
            columns=schema.get_columns(),
        ).schematize(
            schema=schema,
            skip_bad_rows=skip_bad_rows,
            skip_bad_values=skip_bad_values,
            verbose=verbose,
        )

    def get_key_value_pairs(self, key, value, **kwargs) -> Iterable:
        for i in self.get_records():
            k = sf.value_from_record(i, key, **kwargs)
            v = i if value is None else sf.value_from_record(i, value, **kwargs)
            yield k, v

    def to_key_value_stream(self, key='key', value=None, skip_errors=False) -> Pairs:
        kwargs = dict(logger=self.get_logger(), skip_errors=skip_errors)
        pairs = self.get_key_value_pairs(key, value, **kwargs)
        stream = self.stream(
            list(pairs) if self.is_in_memory() else pairs,
            stream_type=sm.StreamType.KeyValueStream,
            value_stream_type=sm.StreamType.RecordStream if value is None else sm.StreamType.AnyStream,
            check=False,
        )
        return self._assume_pairs(stream)

    def to_file(self, file, verbose=True, return_stream=True) -> Stream:
        assert cs.is_file(file), TypeError('Expected TsvFile, got {} as {}'.format(file, type(file)))
        meta = self.get_meta()
        if not file.gzip:
            meta.pop('count')
        file.write_stream(self, verbose=verbose)
        if return_stream:
            return file.to_record_stream(verbose=verbose).update_meta(**meta)

    @deprecated_with_alternative('RecordStream.to_file()')
    def to_tsv_file(self, *args, **kwargs) -> Stream:
        return self.to_file(*args, **kwargs)

    def to_column_file(
            self, filename: str, columns: Union[Iterable, arg.DefaultArgument] = arg.DEFAULT,
            add_title_row=True, gzip=False,
            delimiter='\t', encoding=arg.DEFAULT,
            check=True, verbose=True,
            return_stream=True,
    ) -> Optional[Stream]:
        encoding = arg.delayed_undefault(encoding, self.get_encoding)
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
    ) -> Stream:
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
    ) -> Stream:
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

    @staticmethod
    def _assume_native(stream) -> Native:
        return stream

    @staticmethod
    def _assume_pairs(stream) -> Pairs:
        return stream

    def get_dict(self, key, value=None, of_lists=False, skip_errors=False) -> dict:
        key_value_stream = self.to_key_value_stream(
            key, value,
            skip_errors=skip_errors,
        )
        return key_value_stream.get_dict(
            of_lists=of_lists,
        )

    def get_description(self) -> str:
        return '{} rows, {} columns: {}'.format(
            self.get_str_count(),
            self.get_column_count(),
            ', '.join(self.get_columns()),
        )

    def __str__(self):
        title = self.__repr__()
        description = self.get_description()
        if description:
            return '<{title} {desc}>'.format(title=title, desc=description)
        else:
            return '<{}>'.format(title)
