from abc import ABC, abstractmethod
from typing import Optional, Iterable, Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg, dates as dt
    from interfaces import (
        LeafConnectorInterface, StructInterface, Stream, ItemType,
        AUTO, Auto, AutoCount, AutoBool, Columns, Array,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg, dates as dt
    from ...interfaces import (
        LeafConnectorInterface, StructInterface, Stream, ItemType,
        AUTO, Auto, AutoCount, AutoBool, Columns, Array,
    )

Native = LeafConnectorInterface

EXAMPLE_STR_LEN = 12
COUNT_ITEMS_TO_LOG_COLLECT_OPERATION = 500000


class AppropriateInterface(LeafConnectorInterface, ABC):
    @abstractmethod
    def get_modification_timestamp(self, reset: bool = True) -> float:
        pass

    @abstractmethod
    def get_prev_modification_timestamp(self) -> Optional[float]:
        pass

    @abstractmethod
    def set_prev_modification_timestamp(self, timestamp: float):
        pass

    @abstractmethod
    def get_expected_count(self) -> Union[int, arg.Auto, None]:
        pass

    @abstractmethod
    def set_count(self, count: int):
        pass

    @abstractmethod
    def is_gzip(self) -> bool:
        pass

    @abstractmethod
    def is_verbose(self) -> bool:
        pass

    @abstractmethod
    def is_opened(self) -> bool:
        pass

    @abstractmethod
    def is_closed(self) -> bool:
        pass

    @abstractmethod
    def open(self, allow_reopen: bool):
        pass

    @abstractmethod
    def close(self) -> int:
        pass

    @abstractmethod
    def get_lines(
            self,
            count: Optional[int] = None,
            skip_first: bool = False, allow_reopen: bool = True,
            check: bool = True, verbose: AutoBool = AUTO,
            message: Union[str, Auto] = AUTO, step: AutoCount = AUTO,
    ) -> Iterable:
        pass

    @abstractmethod
    def get_chunks(self) -> Iterable:
        pass

    @abstractmethod
    def set_struct(self, struct: StructInterface, inplace: bool):
        pass

    @abstractmethod
    def get_struct(self) -> StructInterface:
        pass

    @abstractmethod
    def get_initial_struct(self) -> StructInterface:
        pass

    @abstractmethod
    def get_detected_struct_by_title_row(self, set_struct: bool, verbose: bool) -> StructInterface:
        pass

    @abstractmethod
    def _get_native_struct(self, struct) -> StructInterface:
        pass

    @abstractmethod
    def get_columns(self) -> Iterable:
        pass

    @abstractmethod
    def get_column_count(self) -> int:
        pass

    @abstractmethod
    def get_one_item(self, item_type: ItemType):
        pass

    @abstractmethod
    def is_first_line_title(self) -> bool:
        pass

    @abstractmethod
    def is_empty(self) -> bool:
        pass

    @abstractmethod
    def to_record_stream(self) -> Stream:
        pass


class ActualizeMixin(AppropriateInterface, ABC):
    def is_changed_by_another(self) -> bool:
        return not self.is_actual()

    def is_actual(self) -> bool:
        return self.get_modification_timestamp() == self.get_prev_modification_timestamp()

    def actualize(self) -> Native:
        self.get_modification_timestamp()
        self.get_count(force=True)
        return self

    def get_modification_time_str(self) -> str:
        timestamp = dt.datetime.fromtimestamp(self.get_modification_timestamp())
        return dt.get_formatted_datetime(timestamp)

    def reset_modification_timestamp(self, timestamp: Union[float, Auto, None] = AUTO) -> Native:
        timestamp = arg.acquire(timestamp, self.get_modification_timestamp(reset=False))
        return self.set_prev_modification_timestamp(timestamp)

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

    def get_datetime_str(self) -> str:
        if self.is_existing():
            times = self.get_modification_time_str(), self.get_file_age_str(), dt.get_current_time_str()
            return '{} + {} = {}'.format(*times)
        else:
            return dt.get_current_time_str()

    def get_prev_lines_count(self) -> Optional[AutoCount]:
        return self.get_expected_count()

    def get_slow_lines_count(self, verbose: AutoBool = AUTO) -> int:
        count = 0
        for _ in self.get_lines(message='Slow counting lines in {}...', allow_reopen=True, verbose=verbose):
            count += 1
        self.set_count(count)
        return count

    def get_fast_lines_count(self, ending: Union[str, Auto] = AUTO, verbose: AutoBool = AUTO) -> int:
        if self.is_gzip():
            raise ValueError('get_fast_lines_count() method is not available for gzip-files')
        if not arg.is_defined(ending):
            if hasattr(self, 'get_content_format'):
                ending = self.get_content_format().get_ending()
            else:
                ending = '\n'
        verbose = arg.acquire(verbose, self.is_verbose())
        self.log('Counting lines in {}...'.format(self.get_name()), end='\r', verbose=verbose)
        count_n_symbol = sum(chunk.count(ending) for chunk in self.get_chunks())
        count_lines = count_n_symbol + 1
        self.set_count(count_lines)
        return count_lines

    def get_actual_lines_count(self, allow_reopen: bool = True, allow_slow_gzip: bool = True) -> Optional[int]:
        if self.is_opened():
            if allow_reopen:
                self.close()
            else:
                raise ValueError('File is already opened: {}'.format(self))
        self.open(allow_reopen=allow_reopen)
        if self.is_gzip():
            if allow_slow_gzip:
                count = self.get_slow_lines_count()
            else:
                count = None
        else:
            count = self.get_fast_lines_count()
        self.close()
        if count is not None:
            self.log('Detected {} lines in {}.'.format(count, self.get_name()), end='\r')
        return count

    def get_count(self, allow_reopen: bool = True, allow_slow_gzip: bool = True, force: bool = False) -> Optional[int]:
        must_recount = force or self.is_changed_by_another() or not arg.is_defined(self.get_prev_lines_count())
        if self.is_existing() and must_recount:
            count = self.get_actual_lines_count(allow_reopen=allow_reopen, allow_slow_gzip=allow_slow_gzip)
            self.set_count(count)
        else:
            count = self.get_prev_lines_count()
        if arg.is_defined(count):
            return count

    def validate_fields(self, initial: bool = True) -> Native:
        if initial:
            expected_struct = self.get_initial_struct().copy()
        else:
            expected_struct = self.get_struct()
        actual_struct = self.get_detected_struct_by_title_row(set_struct=False, verbose=False)
        actual_struct = self._get_native_struct(actual_struct)
        validated_struct = actual_struct.validate_about(expected_struct)
        self.set_struct(validated_struct, inplace=True)
        return self

    def get_invalid_columns(self) -> Iterable:
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
        row_count = self.get_count(allow_slow_gzip=False)
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

    def get_str_description(self) -> str:
        if self.is_existing():
            rows_count = self.get_count(allow_slow_gzip=False)
            if rows_count:
                cols_count = self.get_column_count() or 0
                invalid_count = self.get_invalid_fields_count() or 0
                valid_count = cols_count - invalid_count
                message = '{} rows, {} columns = {} valid + {} invalid'
                return message.format(rows_count, cols_count, valid_count, invalid_count)
            else:
                message = 'empty file, expected {} columns: {}'
        else:
            message = 'file not exists, expected {} columns: {}'
        return message.format(self.get_column_count(), ', '.join(self.get_columns()))

    def get_str_headers(self) -> Iterable:
        yield "{}('{}') {}".format(self.__class__.__name__, self.get_name(), self.get_str_description())

    def has_title(self) -> bool:
        if self.is_first_line_title():
            if self.is_existing():
                return bool(self.get_count(allow_slow_gzip=False))
        return False

    def get_useful_props(self) -> dict:
        if self.is_existing():
            return dict(
                is_actual=self.is_actual(),
                is_valid=self.is_valid_struct(),
                has_title=self.is_first_line_title(),
                is_opened=self.is_opened(),
                is_empty=self.is_empty(),
                count=self.get_count(allow_slow_gzip=False),
                path=self.get_path(),
            )
        else:
            return dict(
                is_existing=self.is_existing(),
                path=self.get_path(),
            )

    @staticmethod
    def _format_args(*args, **kwargs) -> str:
        formatted_args = list(args) + ['{}={}'.format(k, v) for k, v in kwargs.items()]
        return ', '.join(map(str, formatted_args))

    def _prepare_examples(self, *filters, safe_filter: bool = True, **filter_kwargs) -> tuple:
        filters = filters or list()
        if filter_kwargs and safe_filter:
            filter_kwargs = {k: v for k, v in filter_kwargs.items() if k in self.get_columns()}
        verbose = self.is_gzip() or self.get_count(allow_slow_gzip=False) > COUNT_ITEMS_TO_LOG_COLLECT_OPERATION
        stream_example = self.filter(*filters or [], **filter_kwargs, verbose=verbose)
        item_example = stream_example.get_one_item()
        str_filters = self._format_args(*filters, **filter_kwargs)
        if item_example:
            if str_filters:
                message = 'Example with filters: {}'.format(str_filters)
            else:
                message = 'Example without any filters:'
        else:
            message = '[EXAMPLE_NOT_FOUND] Example with this filters not found: {}'.format(str_filters)
            stream_example = None
            item_example = self.get_one_item(ItemType.Record)
        if item_example:
            if EXAMPLE_STR_LEN:
                for k, v in item_example.items():
                    v = str(v)
                    if len(v) > EXAMPLE_STR_LEN:
                        item_example[k] = str(v)[:EXAMPLE_STR_LEN] + '..'
        else:
            item_example = dict()
            stream_example = None
            message = '[EMPTY_DATA] There are no valid records in stream_dataset {}'.format(self.__repr__())
        return item_example, stream_example, message

    def show(self, count: int = 10, filters: Columns = None, columns: Columns = AUTO, recount: bool = False, **kwargs):
        if recount:
            self.actualize()
        return self.to_record_stream().show(count=count, filters=filters or list(), columns=columns)

    def show_example(
            self, count: int = 10,
            example: Optional[Stream] = None,
            columns: Optional[Array] = None,
            comment: str = '',
    ):
        if not arg.is_defined(example):
            example = self.to_record_stream()
        stream_example = example.take(count).collect()
        if comment:
            self.log('')
            self.log(comment)
        if stream_example:
            example = stream_example.get_demo_example(columns=columns)
            is_dataframe = hasattr(example, 'shape')
            if is_dataframe:
                return example
            else:
                for line in example:
                    self.log(line)

    def describe(
            self, *filter_args,
            count: Optional[int] = 10,
            columns: Optional[Array] = None,
            show_header: bool = True,
            struct_as_dataframe: bool = False,
            safe_filter: bool = True,
            **filter_kwargs
    ):
        if show_header:
            for line in self.get_str_headers():
                self.log(line)
        example_item, example_stream, example_comment = dict(), None, ''
        if self.is_existing():
            self.actualize()
            if self.is_empty():
                message = '[EMPTY] file is empty, expected {} columns:'.format(self.get_column_count())
            else:
                message = self.get_validation_message()
                example_tuple = self._prepare_examples(safe_filter=safe_filter, filters=filter_args, **filter_kwargs)
                example_item, example_stream, example_comment = example_tuple
        else:
            message = '[NOT_EXISTS] file is not created yet, expected {} columns:'.format(self.get_column_count())
        if show_header:
            self.log('{} {}'.format(self.get_datetime_str(), message))
            if self.get_invalid_fields_count():
                self.log('Invalid columns: {}'.format(self._format_args(*self.get_invalid_columns())))
            self.log('')
        struct = self.get_struct()
        dataframe = struct.describe(
            as_dataframe=struct_as_dataframe, example=example_item,
            logger=self.get_logger(), comment=example_comment,
        )
        if dataframe is not None:
            return dataframe
        if example_stream and count:
            return self.show_example(
                count=count, example=example_stream,
                columns=columns, comment=example_comment,
            )
