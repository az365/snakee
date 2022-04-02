from abc import ABC, abstractmethod
from typing import Optional, Generator, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        LeafConnectorInterface, StructInterface, Stream, RecordStream, ItemType,
        AUTO, Auto, AutoCount, AutoBool, Columns, Array, Count,
    )
    from base.functions.arguments import get_name, get_str_from_args_kwargs
    from base.constants.chars import EMPTY, CROP_SUFFIX, ITEMS_DELIMITER, DEFAULT_LINE_LEN
    from base.mixin.line_output_mixin import AutoOutput
    from functions.primary import dates as dt
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        LeafConnectorInterface, StructInterface, Stream, RecordStream, ItemType,
        AUTO, Auto, AutoCount, AutoBool, Columns, Array, Count,
    )
    from ...base.functions.arguments import get_name, get_str_from_args_kwargs
    from ...base.constants.chars import EMPTY, CROP_SUFFIX, ITEMS_DELIMITER, DEFAULT_LINE_LEN
    from ...base.mixin.line_output_mixin import AutoOutput
    from ...functions.primary import dates as dt

Native = LeafConnectorInterface

EXAMPLE_STR_LEN = 12
EXAMPLE_ROW_COUNT = 10
COUNT_ITEMS_TO_LOG_COLLECT_OPERATION = 500000


class AppropriateInterface(LeafConnectorInterface, ABC):
    @abstractmethod
    def get_modification_timestamp(self, reset: bool = True) -> Optional[float]:
        pass

    @abstractmethod
    def get_actual_lines_count(self, allow_reopen: bool = True, allow_slow_mode: bool = True) -> AutoCount:
        pass

    @abstractmethod
    def is_gzip(self) -> bool:
        pass

    @abstractmethod
    def is_opened(self) -> bool:
        pass

    @abstractmethod
    def set_struct(self, struct: StructInterface, inplace: bool):
        pass

    @abstractmethod
    def get_struct(self) -> StructInterface:
        pass

    @abstractmethod
    def get_initial_struct(self) -> StructInterface:
        """Returns initial expected struct from connector settings."""
        pass

    @abstractmethod
    def is_first_line_title(self) -> bool:
        pass

    @abstractmethod
    def is_empty(self) -> bool:
        pass

    @abstractmethod
    def to_record_stream(self, message: Optional[str] = None) -> RecordStream:
        pass


class ActualizeMixin(ABC):
    def is_outdated(self) -> bool:
        return not self.is_actual()

    def is_actual(self) -> bool:
        return self.get_modification_timestamp() == self.get_prev_modification_timestamp()

    def actualize(self, if_outdated: bool = False) -> Native:
        self.get_modification_timestamp()
        if self.is_outdated() or not if_outdated:
            self.get_count(force=True)
            self.get_detected_format(force=True, skip_missing=True)
        return self

    def get_modification_time_str(self) -> str:
        timestamp = dt.datetime.fromtimestamp(self.get_modification_timestamp())
        return dt.get_formatted_datetime(timestamp)

    def reset_modification_timestamp(self, timestamp: Union[float, Auto, None] = AUTO) -> Native:
        timestamp = Auto.acquire(timestamp, self.get_modification_timestamp(reset=False))
        return self.set_prev_modification_timestamp(timestamp) or self

    def get_file_age_str(self):
        timestamp = self.get_modification_timestamp()
        if timestamp:
            timedelta_age = dt.datetime.now() - dt.datetime.fromtimestamp(timestamp)
            assert isinstance(timedelta_age, dt.timedelta)
            if timedelta_age.seconds == 0:
                return 'now'
            elif timedelta_age.seconds > 0:
                return dt.get_str_from_timedelta(timedelta_age)
            else:
                return 'future'

    def get_datetime_str(self, actualize: bool = True) -> str:
        if actualize:
            if self.is_existing():
                times = self.get_modification_time_str(), self.get_file_age_str(), dt.get_current_time_str()
                return '{} + {} = {}'.format(*times)
        return dt.get_current_time_str()

    @staticmethod
    def _get_current_timestamp() -> float:
        return dt.get_current_timestamp()

    def get_prev_lines_count(self) -> Optional[AutoCount]:
        return self.get_expected_count()

    def get_count(self, allow_reopen: bool = True, allow_slow_mode: bool = True, force: bool = False) -> Count:
        must_recount = force or self.is_outdated() or not Auto.is_defined(self.get_prev_lines_count())
        if self.is_existing() and must_recount:
            count = self.get_actual_lines_count(allow_reopen=allow_reopen, allow_slow_mode=allow_slow_mode)
            self.set_count(count)
        else:
            count = self.get_prev_lines_count()
        if Auto.is_defined(count):
            return count

    def has_title(self) -> bool:
        if self.is_first_line_title():
            if self.is_existing():
                return bool(self.get_count(allow_slow_mode=False))
        return False

    def validate_fields(self, initial: bool = True) -> Native:
        if initial:
            expected_struct = self.get_initial_struct()
            if Auto.is_defined(expected_struct):
                expected_struct = expected_struct.copy()
            else:
                expected_struct = self.get_struct_from_source(set_struct=True, verbose=True)
        else:
            expected_struct = self.get_struct()
        actual_struct = self.get_struct_from_source(set_struct=False, verbose=False)
        actual_struct = self._get_native_struct(actual_struct)
        validated_struct = actual_struct.validate_about(expected_struct)
        self.set_struct(validated_struct, inplace=True)
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

    def get_validation_message(self) -> str:
        self.validate_fields()
        row_count = self.get_count(allow_slow_mode=False)
        column_count = self.get_column_count()
        error_count = self.get_invalid_fields_count()
        if self.is_valid_struct():
            message = 'file has {} rows, {} valid columns:'.format(row_count, column_count)
        else:
            valid_count = column_count - error_count
            template = '[INVALID] file has {} rows, {} columns = {} valid + {} invalid:'
            message = template.format(row_count, column_count, valid_count, error_count)
        if not hasattr(self.get_struct(), 'get_caption'):
            message = '[DEPRECATED] {}'.format(message)
        return message

    def get_shape_repr(self, actualize: bool = False) -> str:
        return self.get_columns_repr(actualize=actualize)

    def get_columns_repr(self, actualize: bool = True) -> str:
        if actualize:
            if self.is_existing():
                rows_count = self.get_count(allow_slow_mode=False)
                if rows_count:
                    cols_count = self.get_column_count() or 0
                    invalid_count = self.get_invalid_fields_count() or 0
                    valid_count = cols_count - invalid_count
                    message = '{} rows, {} columns = {} valid + {} invalid'
                    return message.format(rows_count, cols_count, valid_count, invalid_count)
                else:
                    message = 'empty dataset, expected {count} columns: {columns}'
            else:
                message = 'dataset not exists, expected {count} columns: {columns}'
        else:
            message = 'expected'
        return message.format(count=self.get_column_count(), columns=ITEMS_DELIMITER.join(self.get_columns()))

    def get_str_headers(self, actualize: bool = False) -> Generator:
        cls = self.__class__.__name__
        name = self.get_name()
        shape = self.get_shape_repr(actualize=actualize)
        str_header = "{cls}('{name}') {shape}".format(cls=cls, name=name, shape=shape)
        if len(str_header) > DEFAULT_LINE_LEN:
            str_header = str_header[:DEFAULT_LINE_LEN - len(CROP_SUFFIX)] + CROP_SUFFIX
        yield str_header

    def get_useful_props(self) -> dict:
        if self.is_existing():
            return dict(
                is_actual=self.is_actual(),
                is_valid=self.is_valid_struct(),
                has_title=self.is_first_line_title(),
                is_opened=self.is_opened(),
                is_empty=self.is_empty(),
                count=self.get_count(allow_slow_mode=False),
                path=self.get_path(),
            )
        else:
            return dict(
                is_existing=self.is_existing(),
                path=self.get_path(),
            )

    def get_one_line_repr(
            self,
            str_meta: Union[str, Auto, None] = AUTO,
            max_len: int = DEFAULT_LINE_LEN,
            crop: str = CROP_SUFFIX,
    ) -> str:
        if not Auto.is_defined(str_meta):
            description_args = list()
            name = get_name(self)
            if name:
                description_args.append(name)
            if self.get_str_count(default=None) is not None:
                description_args.append(self.get_shape_repr())
            str_meta = get_str_from_args_kwargs(*description_args)
        return super().get_one_line_repr(str_meta=str_meta, max_len=max_len, crop=crop)

    def _prepare_examples_with_title(
            self,
            *filters,
            safe_filter: bool = True,
            example_row_count: Count = None,
            example_str_len: int = EXAMPLE_STR_LEN,
            actualize: AutoBool = AUTO,
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
        if is_empty:
            message = '[EMPTY] dataset is empty, expected {} columns:'.format(self.get_column_count())
        elif is_existing:
            message = self.get_validation_message()
            example_tuple = self._prepare_examples(
                *filters, **filter_kwargs, safe_filter=safe_filter,
                example_row_count=example_row_count, example_str_len=example_str_len,
            )
            example_item, example_stream, example_comment = example_tuple
        elif is_actual:
            message = '[NOT_EXISTS] dataset is not created yet, expected {} columns:'.format(self.get_column_count())
        else:
            message = '[EXPECTED] connection not established, expected {} columns:'.format(self.get_column_count())
        title = '{time} {msg}'.format(time=self.get_datetime_str(actualize=actualize), msg=message)
        return title, example_item, example_stream, example_comment

    def _prepare_examples(
            self,
            *filters,
            safe_filter: bool = True,
            example_row_count: Count = None,
            example_str_len: int = EXAMPLE_STR_LEN,
            crop_suffix: str = CROP_SUFFIX,
            **filter_kwargs
    ) -> tuple:
        filters = filters or list()
        if filter_kwargs and safe_filter:
            filter_kwargs = {k: v for k, v in filter_kwargs.items() if k in self.get_columns()}
        stream_example = self.to_record_stream()
        if filters:
            stream_example = stream_example.filter(*filters or [], **filter_kwargs)
        if example_row_count:
            stream_example = stream_example.take(example_row_count).collect()
        item_example = stream_example.get_one_item()
        str_filters = get_str_from_args_kwargs(*filters, **filter_kwargs)
        if item_example:
            if str_filters:
                message = 'Example with filters: {}'.format(str_filters)
            else:
                message = 'Example without any filters:'
        else:
            message = '[EXAMPLE_NOT_FOUND] Example with this filters not found: {}'.format(str_filters)
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
            message = '[EMPTY_DATA] There are no valid records in stream_dataset {}'.format(self.__repr__())
        return item_example, stream_example, message

    def show_example(
            self,
            count: int = EXAMPLE_ROW_COUNT,
            example: Optional[Stream] = None,
            columns: Optional[Array] = None,
            comment: str = EMPTY,
            output: AutoOutput = AUTO,
    ):
        if not Auto.is_defined(example):
            example = self.to_record_stream()
        stream_example = example.take(count).collect()
        if comment:
            self.output_line(EMPTY, output=output)
            self.output_line(comment, output=output)
        if stream_example:
            example = stream_example.get_demo_example(columns=columns)
            is_dataframe = hasattr(example, 'shape')
            if is_dataframe:
                return example
            else:
                for line in example:
                    self.output_line(line, output=output)

    def show(
            self,
            count: int = EXAMPLE_ROW_COUNT,
            message: Optional[str] = None,
            filters: Columns = None,
            columns: Columns = None,
            actualize: AutoBool = AUTO,
            as_dataframe: AutoBool = AUTO,
            **kwargs
    ):
        if actualize == AUTO:
            self.actualize(if_outdated=True)
        elif actualize:
            self.actualize(if_outdated=False)
        return self.to_record_stream(message=message).show(
            count=count, as_dataframe=as_dataframe,
            filters=filters or list(), columns=columns,
        )

    def describe(
            self,
            *filter_args,
            count: Optional[int] = EXAMPLE_ROW_COUNT,
            columns: Optional[Array] = None,
            show_header: bool = True,
            struct_as_dataframe: bool = False,
            safe_filter: bool = True,
            actualize: AutoBool = AUTO,
            output: AutoOutput = AUTO,
            **filter_kwargs
    ):
        output = Auto.acquire(output, print)
        if show_header:
            for line in self.get_str_headers(actualize=False):
                self.output_line(line, output=output)
            struct_title, example_item, example_stream, example_comment = self._prepare_examples_with_title(
                *filter_args, **filter_kwargs, safe_filter=safe_filter,
                example_row_count=count, actualize=actualize,
            )
            self.output_line(struct_title, output=output)
            if self.get_invalid_fields_count():
                line = 'Invalid columns: {}'.format(get_str_from_args_kwargs(*self.get_invalid_columns()))
                self.output_line(line, output=output)
            self.output_blank_line(output=output)
        struct = self.get_struct()
        struct_dataframe = struct.describe(
            show_header=False,
            as_dataframe=struct_as_dataframe, example=example_item,
            output=output, comment=example_comment,
        )
        if struct_dataframe is not None:
            return struct_dataframe
        if example_stream and count:
            return self.show_example(
                count=count, example=example_stream,
                columns=columns, comment=example_comment,
                output=output,
            )
