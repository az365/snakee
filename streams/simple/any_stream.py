try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as sm
    from utils import (
        arguments as arg,
        items as it,
        selection as sf,
    )
    from selection import selection_classes as sn
    from loggers.logger_classes import deprecated_with_alternative
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import stream_classes as sm
    from ...utils import (
        arguments as arg,
        items as it,
        selection as sf,
    )
    from ...selection import selection_classes as sn
    from ...loggers.logger_classes import deprecated_with_alternative


class AnyStream(sm.LocalStream, sm.ConvertMixin):
    def __init__(
            self,
            data,
            name=arg.DEFAULT,
            count=None, less_than=None,
            source=None, context=None,
            check=False,
            max_items_in_memory=arg.DEFAULT,
            tmp_files_template=arg.DEFAULT,
            tmp_files_encoding=arg.DEFAULT,
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

    @deprecated_with_alternative('map()')
    def native_map(self, function):
        return self.__class__(
            map(function, self.get_items()),
            self.get_meta(),
        )

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
        return sm.LineStream.from_text_file(
            filename,
            encoding=encoding, gzip=gzip,
            skip_first_line=skip_first_line, max_count=max_count,
            check=check,
            verbose=verbose,
        ).parse_json(
            to=cls.__name__,
        )
