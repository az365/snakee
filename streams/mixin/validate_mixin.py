from abc import ABC
from typing import Optional, Generator, Tuple, Union

try:  # Assume we're a submodule in a package.
    from interfaces import LeafConnectorInterface, Stream, Item, Columns, Array, Count, AutoBool, Auto, AUTO
    from base.functions.arguments import get_str_from_args_kwargs
    from base.constants.chars import EMPTY, CROP_SUFFIX
    from streams.interfaces.abstract_stream_interface import StreamInterface, DEFAULT_EXAMPLE_COUNT
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import LeafConnectorInterface, Stream, Item, Columns, Array, Count, AutoBool, Auto, AUTO
    from ...base.functions.arguments import get_str_from_args_kwargs
    from ...base.constants.chars import EMPTY, CROP_SUFFIX
    from ..interfaces.abstract_stream_interface import StreamInterface, DEFAULT_EXAMPLE_COUNT

Native = Union[StreamInterface, LeafConnectorInterface]

EXAMPLE_STR_LEN = 12


class ValidateMixin(ABC):
    def validate_fields(self, initial: bool = True, skip_disconnected: bool = False) -> Native:
        if initial:
            expected_struct = self.get_initial_struct()
            if Auto.is_defined(expected_struct):
                expected_struct = expected_struct.copy()
            else:
                expected_struct = self.get_struct_from_source(set_struct=True, verbose=True)
        else:
            expected_struct = self.get_struct()
        actual_struct = self.get_struct_from_source(set_struct=False, verbose=False)
        if actual_struct:
            actual_struct = self._get_native_struct(actual_struct)
            validated_struct = actual_struct.validate_about(expected_struct)
            self.set_struct(validated_struct, inplace=True)
        elif not skip_disconnected:
            raise ValueError(f'For validate fields storage/database must be connected: {self.get_storage()}')
        return self

    def get_invalid_columns(self) -> Generator:
        struct = self.get_struct()
        if hasattr(struct, 'get_fields'):
            for f in struct.get_fields():
                if hasattr(f, 'is_valid'):
                    if not f.is_valid():
                        yield f

    def get_invalid_fields_count(self) -> int:
        count = 0
        for _ in self.get_invalid_columns():
            count += 1
        return count

    def is_valid_struct(self) -> bool:
        for _ in self.get_invalid_columns():
            return False
        return True

    def get_validation_message(self, skip_disconnected: bool = True) -> str:
        if self.is_accessible():
            self.validate_fields()
            row_count = self.get_count(allow_slow_mode=False)
            column_count = self.get_column_count()
            error_count = self.get_invalid_fields_count()
            if self.is_valid_struct():
                message = f'file has {row_count} rows, {column_count} valid columns:'
            else:
                tag = '[INVALID]'
                valid_count = column_count - error_count
                validation_str = f'{valid_count} valid + {error_count} invalid'
                message = f'{tag} file has {row_count} rows, {column_count} columns = {validation_str}:'
            if not hasattr(self.get_struct(), 'get_caption'):
                tag = '[DEPRECATED]'
                message = f'{tag} {message}'
        else:
            message = 'Cannot validate struct while dataset source is disconnected'
            if skip_disconnected:
                tag = '[DISCONNECTED]'
                message = f'{tag} {message}'
            else:
                message = f'{self}: {message}: {self.get_source()}'
                raise ValueError(message)
        return message

    def _get_demo_example(
            self,
            count: int = DEFAULT_EXAMPLE_COUNT,
            filters: Columns = None,
            columns: Columns = None,
            example: Optional[Stream] = None,
    ) -> Native:
        if hasattr(self, 'is_in_memory'):  # isinstance(self, IterableStream)
            is_in_memory = self.is_in_memory()
        else:
            is_in_memory = True  # ?
        if Auto.is_defined(example):
            stream = example
        elif is_in_memory:
            stream = self
        elif hasattr(self, 'copy'):  # data is iterator
            stream = self.copy()
        else:
            stream = self
        if Auto.is_defined(filters):
            stream = stream.filter(*filters)
        if Auto.is_defined(count):
            stream = stream.take(count)
        if Auto.is_defined(columns) and hasattr(stream, 'select'):
            stream = stream.select(columns)
        return stream.collect()

    def _get_demo_records_and_columns(
            self,
            count: int = DEFAULT_EXAMPLE_COUNT,
            filters: Columns = None,
            columns: Optional[Array] = None,
            example: Optional[Stream] = None,
    ) -> Tuple[Array, Array]:
        example = self._get_demo_example(count=count, filters=filters, columns=columns, example=example)
        if hasattr(example, 'get_columns') and hasattr(example, 'get_records'):  # RegularStream, SqlStream
            records = example.get_records()  # ConvertMixin.get_records(), SqlStream.get_records()
            columns = example.get_columns()  # StructMixin.get_columns(), RegularStream.get_columns()
        else:
            item_field = 'item'
            records = [{item_field: i} for i in example]
            columns = [item_field]
        return records, columns

    def _prepare_examples_with_title(
            self,
            *filters,
            safe_filter: bool = True,
            example_row_count: Count = None,
            example_str_len: int = EXAMPLE_STR_LEN,
            actualize: AutoBool = AUTO,
            verbose: bool = True,
            **filter_kwargs
    ) -> tuple:
        example_item, example_stream, example_comment = dict(), None, EMPTY
        is_existing, is_empty = None, None
        is_actual = self.is_actual()
        actualize = Auto.acquire(actualize, not is_actual)
        if actualize or is_actual:
            is_existing = self.is_existing()
        if actualize and is_existing:
            self.actualize()
            is_actual = True
            is_empty = self.is_empty()
        columns_count = self.get_column_count()
        if is_empty:
            message = f'[EMPTY] dataset is empty, expected {columns_count} columns:'
        elif is_existing:
            message = self.get_validation_message()
            example_tuple = self._prepare_examples(
                *filters, **filter_kwargs, safe_filter=safe_filter,
                example_row_count=example_row_count, example_str_len=example_str_len,
                verbose=verbose,
            )
            example_item, example_stream, example_comment = example_tuple
        elif is_actual:
            message = f'[NOT_EXISTS] dataset is not created yet, expected {columns_count} columns:'
        else:
            message = f'[EXPECTED] connection not established, expected {columns_count} columns:'
        if isinstance(self, LeafConnectorInterface) or hasattr(self, 'get_datetime_str'):  # const True
            current_time = self.get_datetime_str(actualize=actualize)
            title = f'{current_time} {message}'
        else:
            title = message
        return title, example_item, example_stream, example_comment

    def _prepare_examples(
            self,
            *filters,
            safe_filter: bool = True,
            example_row_count: Count = None,
            example_str_len: int = EXAMPLE_STR_LEN,
            crop_suffix: str = CROP_SUFFIX,
            verbose: bool = AUTO,
            **filter_kwargs
    ) -> Tuple[Item, Stream, str]:
        filters = filters or list()
        if filter_kwargs and safe_filter:
            filter_kwargs = {k: v for k, v in filter_kwargs.items() if k in self.get_columns()}
        stream_example = self.to_record_stream(verbose=verbose)
        if filters:
            stream_example = stream_example.filter(*filters or [], **filter_kwargs)
        if example_row_count:
            stream_example = stream_example.take(example_row_count).collect()
        item_example = stream_example.get_one_item()
        str_filters = get_str_from_args_kwargs(*filters, **filter_kwargs)
        if item_example:
            if str_filters:
                message = f'Example with filters: {str_filters}'
            else:
                message = 'Example without any filters:'
        else:
            tag = '[EXAMPLE_NOT_FOUND]'
            message = f'{tag} Example with this filters not found: {str_filters}'
            stream_example = None
            if hasattr(self, 'get_one_record'):
                item_example = self.get_one_record()
            elif hasattr(self, 'get_one_item'):
                item_example = self.get_one_item()
            else:
                item_example = dict()
        if item_example:
            if example_str_len:
                for k, v in item_example.items():
                    v = str(v)
                    if len(v) > example_str_len:
                        item_example[k] = str(v)[:example_str_len - len(crop_suffix)] + crop_suffix
        else:
            item_example = dict()
            stream_example = None
            tag = '[EMPTY_DATA]'
            message = f'{tag} There are no valid items in stream_dataset {repr(self)}'
        return item_example, stream_example, message
