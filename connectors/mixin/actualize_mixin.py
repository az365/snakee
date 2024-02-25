from abc import ABC
from typing import Optional

try:  # Assume we're a submodule in a package.
    from interfaces import LeafConnectorInterface, Stream, Columns, Count
    from base.constants.chars import CROP_SUFFIX, ITEMS_DELIMITER
    from base.constants.text import DEFAULT_LINE_LEN
    from base.functions.arguments import get_name, get_str_from_args_kwargs
    from functions.primary import dates as dt
    from streams.mixin.validate_mixin import ValidateMixin, DEFAULT_EXAMPLE_COUNT
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import LeafConnectorInterface, Stream, Columns, Count
    from ...base.constants.chars import CROP_SUFFIX, ITEMS_DELIMITER
    from ...base.constants.text import DEFAULT_LINE_LEN
    from ...base.functions.arguments import get_name, get_str_from_args_kwargs
    from ...functions.primary import dates as dt
    from ...streams.mixin.validate_mixin import ValidateMixin, DEFAULT_EXAMPLE_COUNT

Native = LeafConnectorInterface


class ActualizeMixin(ValidateMixin, ABC):
    def is_outdated(self) -> bool:
        return not self.is_actual()

    def is_actual(self) -> bool:
        return self.get_modification_timestamp() == self.get_prev_modification_timestamp()

    def actualize(self, if_outdated: bool = False, allow_slow_mode: bool = False) -> Native:
        self.get_modification_timestamp()  # just update property
        if self.is_outdated() or not if_outdated:
            self.get_count(force=True, allow_slow_mode=allow_slow_mode)
            self.get_detected_format(force=True, skip_missing=True)
        return self

    def get_modification_time_str(self) -> str:
        timestamp = dt.datetime.fromtimestamp(self.get_modification_timestamp())
        return dt.get_formatted_datetime(timestamp)

    def reset_modification_timestamp(self, timestamp: Optional[float] = None) -> Native:
        if timestamp is None:
            timestamp = self.get_modification_timestamp(reset=False)
        return self.set_prev_modification_timestamp(timestamp) or self

    def get_file_age_str(self):
        timestamp = self.get_modification_timestamp()
        if timestamp:
            timedelta_age = dt.datetime.now() - dt.datetime.fromtimestamp(timestamp)
            assert isinstance(timedelta_age, dt.timedelta), f'Expected timedelta, got {timedelta_age}'
            if timedelta_age.seconds == 0:
                return 'now'
            elif timedelta_age.seconds > 0:
                return dt.get_str_from_timedelta(timedelta_age)
            else:
                return 'future'

    def get_datetime_str(self, actualize: bool = True) -> str:
        if actualize:
            if self.is_existing():
                modification_time = self.get_modification_time_str()
                data_age = self.get_file_age_str()
                current_time = dt.get_current_time_str()
                return f'{modification_time} + {data_age} = {current_time}'
        return dt.get_current_time_str()

    @staticmethod
    def _get_current_timestamp() -> float:
        return dt.get_current_timestamp()

    def get_prev_lines_count(self) -> Count:
        return self.get_expected_count()

    def get_count(self, allow_reopen: bool = True, allow_slow_mode: bool = True, force: bool = False) -> Count:
        prev_lines_count = self.get_prev_lines_count()
        must_recount = force or self.is_outdated() or prev_lines_count is None
        if self.is_existing() and must_recount:
            count = self.get_actual_lines_count(allow_reopen=allow_reopen, allow_slow_mode=allow_slow_mode)
            self.set_count(count)
        else:
            count = prev_lines_count
        return count

    def has_title(self) -> bool:
        if self.is_first_line_title():
            if self.is_existing():
                return bool(self.get_count(allow_slow_mode=False))
        return False

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
        columns = self.get_columns()
        if columns is None:
            columns_list_str = '(undefined)'
        else:
            columns_list_str = ITEMS_DELIMITER.join(columns)
        return message.format(count=self.get_column_count(), columns=columns_list_str)

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
            str_meta: Optional[str] = None,
            max_len: int = DEFAULT_LINE_LEN,
            crop: str = CROP_SUFFIX,
    ) -> str:
        if str_meta is None:
            description_args = list()
            name = get_name(self)
            if name:
                description_args.append(name)
            if self.get_str_count(default=None) is not None:
                description_args.append(self.get_shape_repr())
            str_meta = get_str_from_args_kwargs(*description_args)
        return super().get_one_line_repr(str_meta=str_meta, max_len=max_len, crop=crop)

    def show(
            self,
            count: int = DEFAULT_EXAMPLE_COUNT,
            message: Optional[str] = None,
            filters: Columns = None,
            columns: Columns = None,
            actualize: Optional[bool] = None,
            **kwargs
    ):
        if actualize is None:
            self.actualize(if_outdated=True)
        elif actualize:  # True
            self.actualize(if_outdated=False)
        return self.to_record_stream(message=message).show(
            count=count,
            filters=filters or list(),
            columns=columns,
        )
