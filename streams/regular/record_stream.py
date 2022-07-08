from typing import Optional, Iterable, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        Stream, RegularStream, KeyValueStream,
        ItemType, StreamType,
        Context, Connector, LeafConnectorInterface, TmpFiles,
        Count, Field, Struct, Columns,
        AUTO, Auto, AutoCount, AutoName,
    )
    from utils.decorators import deprecated_with_alternative
    from streams.mixin.convert_mixin import ConvertMixin
    from streams.mixin.columnar_mixin import ColumnarMixin
    from streams.regular.any_stream import AnyStream
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        Stream, RegularStream, KeyValueStream,
        ItemType, StreamType,
        Context, Connector, LeafConnectorInterface, TmpFiles,
        Count, Field, Struct, Columns,
        AUTO, Auto, AutoCount, AutoName,
    )
    from ...utils.decorators import deprecated_with_alternative
    from ..mixin.convert_mixin import ConvertMixin
    from ..mixin.columnar_mixin import ColumnarMixin
    from .any_stream import AnyStream

Native = RegularStream


class RecordStream(AnyStream, ColumnarMixin, ConvertMixin):
    def __init__(
            self,
            data: Iterable,
            name: AutoName = AUTO,
            caption: str = '',
            count: Count = None,
            less_than: Count = None,
            struct: Struct = None,
            source: Connector = None,
            context: Context = None,
            max_items_in_memory: AutoCount = AUTO,
            tmp_files: TmpFiles = AUTO,
            check: bool = False,
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
        return ItemType.Record

    def to_file(self, file: Connector, verbose: bool = True, return_stream: bool = True) -> Native:
        if not (isinstance(file, LeafConnectorInterface) or hasattr(file, 'write_stream')):
            raise TypeError('Expected TsvFile, got {} as {}'.format(file, type(file)))
        meta = self.get_meta()
        file.write_stream(self, verbose=verbose)
        if return_stream:
            return file.to_record_stream(verbose=verbose).update_meta(**meta)

    @deprecated_with_alternative('RecordStream.to_file()')
    def to_tsv_file(self, *args, **kwargs) -> Stream:
        return self.to_file(*args, **kwargs)

    def to_column_file(
            self, filename: str, columns: Union[Iterable, Auto] = AUTO,
            add_title_row=True, delimiter='\t',
            check=True, verbose=True, return_stream=True,
    ) -> Optional[Native]:
        meta = self.get_meta()
        columns = Auto.delayed_acquire(columns, self.get_columns)
        row_stream = self.to_row_stream(columns=columns)
        if add_title_row:
            assert Auto.is_defined(columns)
            row_stream.add_items([columns], before=True, inplace=True)
        line_stream = row_stream.to_line_stream(delimiter=delimiter)
        sm_csv_file = line_stream.to_text_file(
            filename,
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
            filename, columns, delimiter='\t',
            skip_first_line=True, check=AUTO,
            expected_count=AUTO, verbose=True,
    ) -> Native:
        stream_class = StreamType.LineStream.get_class()
        return stream_class.from_text_file(
            filename, skip_first_line=skip_first_line,
            check=check, expected_count=expected_count, verbose=verbose,
        ).to_row_stream(
            delimiter=delimiter,
        ).to_record_stream(
            columns=columns,
        )

    @classmethod
    def from_json_file(
            cls, filename,
            default_value=None, max_count=None,
            check=True, verbose=False,
    ) -> Native:
        stream_class = StreamType.LineStream.get_class()
        parsed_stream = stream_class.from_text_file(
            filename,
            max_count=max_count, check=check, verbose=verbose,
        ).parse_json(
            default_value=default_value,
            to=StreamType.RecordStream,
        )
        return parsed_stream

    @staticmethod
    def _assume_native(stream) -> Native:
        return stream

    @staticmethod
    def _assume_pairs(stream) -> KeyValueStream:
        return stream

    def get_dict(
            self,
            key: Union[Field, Columns],
            value: Union[Field, Columns, None] = None,
            of_lists: bool = False,
            skip_errors: bool = False,
    ) -> dict:
        key_value_stream = self.to_key_value_stream(key, value, skip_errors=skip_errors)
        return key_value_stream.get_dict(of_lists=of_lists)

    def get_str_description(self) -> str:
        return '{} rows, {} columns: {}'.format(
            self.get_str_count(),
            self.get_column_count(),
            ', '.join([str(c) for c in self.get_columns()]),
        )
