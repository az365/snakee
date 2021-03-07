import json

try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as sm
    from utils import (
        arguments as arg,
        items as it,
        selection as sf,
        algo,
    )
    from selection import selection_classes as sn
    from loggers import logger_classes as log
    from loggers.logger_classes import deprecated_with_alternative
    from functions import all_functions as fs
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import stream_classes as sm
    from ...utils import (
        arguments as arg,
        items as it,
        selection as sf,
        algo,
    )
    from ...selection import selection_classes as sn
    from ...loggers import logger_classes as log
    from ...loggers.logger_classes import deprecated_with_alternative
    from ...functions import all_functions as fs


class AnyStream(sm.LocalStream):
    def __init__(
            self,
            data,
            name=arg.DEFAULT,
            count=None,
            less_than=None,
            source=None,
            context=None,
            max_items_in_memory=sm.MAX_ITEMS_IN_MEMORY,
            tmp_files_template=sm.TMP_FILES_TEMPLATE,
            tmp_files_encoding=sm.TMP_FILES_ENCODING,
    ):
        super().__init__(
            data,
            name=name,
            count=count, less_than=less_than,
            source=source, context=context,
            max_items_in_memory=max_items_in_memory,
            tmp_files_template=tmp_files_template,
            tmp_files_encoding=tmp_files_encoding,
        )

    def native_map(self, function):
        return self.__class__(
            map(function, self.get_items()),
            count=self.count,
            less_than=self.count or self.less_than,
        ).set_meta(
            **self.get_meta_except_count(),
        )

    def map_to_any(self, function):
        return AnyStream(
            map(function, self.get_items()),
            count=self.count,
            less_than=self.count or self.less_than,
        )

    def map_to_records(self, function=None):
        def get_record(i):
            if function is None:
                return i if isinstance(i, dict) else dict(item=i)
            else:
                return function(i)
        return sm.RecordStream(
            map(get_record, self.get_items()),
            count=self.count,
            less_than=self.less_than,
            check=True,
        )

    def select(self, *columns, use_extended_method=True, **expressions):
        if columns and not expressions:
            target_stream_type = sm.StreamType.RowStream
            target_item_type = it.ItemType.Row
            input_item_type = it.ItemType.Any
        elif expressions and not columns:
            target_stream_type = sm.StreamType.RecordStream
            target_item_type = it.ItemType.Record
            input_item_type = it.ItemType.Any
        else:
            target_stream_type = sm.StreamType.AnyStream
            target_item_type = it.ItemType.Auto
            input_item_type = it.ItemType.Auto
        if use_extended_method:
            selection_method = sn.select
        else:
            selection_method = sf.select
        select_function = selection_method(
            *columns, **expressions,
            target_item_type=target_item_type, input_item_type=input_item_type,
            logger=self.get_logger(), selection_logger=self.get_selection_logger(),
        )
        return self.map(
            function=select_function,
            to=target_stream_type,
        )

    def to_any(self):
        return sm.AnyStream(
            self.get_items(),
            count=self.count,
            less_than=self.less_than,
        )

    def to_lines(self, **kwargs):
        return sm.LineStream(
            self.map_to_any(str).get_items(),
            count=self.count,
            less_than=self.less_than,
            check=True,
        )

    def to_json(self):
        return self.map_to_any(
            json.dumps
        ).to_lines()

    def to_rows(self, *args, **kwargs):
        function = kwargs.pop('function', None)
        if kwargs:
            message = 'to_rows(): kwargs {} are not supported for class {}'.format(kwargs.keys(), self.get_class_name())
            raise ValueError(message)
        if args:
            message = 'to_rows(): positional arguments are not supported for class {}'.format(self.get_class_name())
            raise ValueError(message)
        return sm.RowStream(
            map(function, self.get_items()) if function is not None else self.get_items(),
            count=self.count,
            less_than=self.less_than,
        )

    def to_pairs(self, key=fs.value_by_key(0), value=fs.value_by_key(1)):
        if isinstance(key, (list, tuple)):
            key = fs.composite_key(key)
        if isinstance(value, (list, tuple)):
            value = fs.composite_key(value)
        pairs_data = self.map(
            lambda i: sf.row_from_any(i, key, value),
        ).get_items()
        return sm.KeyValueStream(
            pairs_data,
            count=self.count,
            less_than=self.less_than,
        )

    def to_records(self, **kwargs):
        function = kwargs.get('function')
        return self.map_to_records(function)

    @classmethod
    @deprecated_with_alternative('connectors.filesystem.local_file.JsonFile.to_stream()')
    def from_json_file(
            cls,
            filename,
            encoding=None, gzip=False,
            skip_first_line=False, max_count=None,
            check=arg.DEFAULT,
            verbose=False,
    ):
        parsed_stream = sm.LineStream.from_text_file(
            filename,
            encoding=encoding, gzip=gzip,
            skip_first_line=skip_first_line, max_count=max_count,
            check=check,
            verbose=verbose,
        ).parse_json(
            to=cls.__name__,
        )
        return parsed_stream
