from typing import Optional, Iterable

try:  # Assume we're a submodule in a package.
    from base.constants.chars import RETURN_CHAR, ITEMS_DELIMITER
    from loggers.detailed_message import DetailedMessage, SelectionError
    from loggers.selection_logger_interface import SelectionLoggerInterface, SELECTION_LOGGER_NAME
    from loggers.extended_logger_interface import LoggingLevel
    from loggers.extended_logger import ExtendedLogger
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..base.constants.chars import RETURN_CHAR, ITEMS_DELIMITER
    from .detailed_message import DetailedMessage, SelectionError
    from .selection_logger_interface import SelectionLoggerInterface, SELECTION_LOGGER_NAME
    from .extended_logger_interface import LoggingLevel
    from .extended_logger import ExtendedLogger

DEFAULT_MESSAGE_COLLECTOR_NAME = 'MessageCollector'


class MessageCollector(ExtendedLogger):
    def __init__(
            self,
            name: str = DEFAULT_MESSAGE_COLLECTOR_NAME,
            level: Optional[LoggingLevel] = None,
            formatter: Optional[str] = None,
            max_line_len: Optional[int] = None,
            context=None,
            max_keys: Optional[int] = None,
            max_items: Optional[int] = None,
    ):
        super().__init__(name=name, level=level, formatter=formatter, max_line_len=max_line_len, context=context)
        self.counts = dict()
        self.examples = dict()
        self.max_keys = max_keys
        self.max_items = max_items

    @staticmethod
    def is_common_logger() -> bool:
        return False

    def get_keys(self) -> Iterable:
        return self.counts.keys()

    def get_key_count(self) -> int:
        return len(self.counts)

    def get_count(self, key) -> Optional[int]:
        return self.counts.get(key)

    def get_counts(self) -> dict:
        return self.counts

    def get_examples(self, key) -> list:
        return self.examples.get(key, [])

    def get_example_count(self, key) -> int:
        return len(self.get_examples(key))

    def add_message(self, message: DetailedMessage, key_as_str=True) -> None:
        if key_as_str:
            key = message.get_key_str()
        else:
            key = message.get_key_values()
        cur_count = self.counts.get(key, 0)
        if cur_count or self.get_key_count() < self.max_keys:
            self.counts[key] = cur_count + 1
        if self.get_example_count(key) < self.max_items:
            details = message.get_detail_values()
            self.get_examples(key).append(details)

    def log(self, msg, level=None, logger=None, end=None, verbose=False, truncate=False, **kwargs) -> None:
        if not isinstance(level, (LoggingLevel, int)):
            level = LoggingLevel.Debug
        if isinstance(msg, str):
            msg = DetailedMessage(message=msg)
        self.add_message(msg)
        if logger or verbose:
            super().log(msg, level, logger, end, verbose, truncate, **kwargs)

    def __str__(self):
        name = self.get_name()
        values = [f'{k}: {v}' for k, v in self.get_counts().items()]
        values_str = ITEMS_DELIMITER.join(values)
        return f'{name}: {values_str}'


class SelectionMessageCollector(MessageCollector, SelectionLoggerInterface):
    def __init__(
            self,
            name: str = SELECTION_LOGGER_NAME,
            level: Optional[LoggingLevel] = None,
            formatter: Optional[str] = None,
            max_line_len: Optional[int] = None,
            context=None,
            max_keys: Optional[int] = 10,
            max_items: Optional[int] = 5,
            ok_key: str = 'ok',
    ):
        super().__init__(
            name=name,
            level=level, formatter=formatter, max_line_len=max_line_len, context=context,
            max_keys=max_keys, max_items=max_items,
        )
        self.ok_key = ok_key
        self.counts[ok_key] = 0

    def get_ok_key(self) -> str:
        return self.ok_key

    def get_err_keys(self) -> list:
        return [i for i in self.get_unordered_keys() if i != self.get_ok_key()]

    def get_unordered_keys(self) -> Iterable:
        return super().get_keys()

    def get_ordered_keys(self) -> list:
        return [self.get_ok_key()] + self.get_err_keys()

    def get_keys(self, ordered=True) -> Iterable:
        if ordered:
            return self.get_ordered_keys()
        else:
            return self.get_unordered_keys()

    def is_new_key(self, message: SelectionError, key_as_str=True) -> bool:
        if key_as_str:
            key = message.get_key_str()
        else:
            key = message.get_key_values()
        if key not in self.get_keys():
            return True

    def get_message_batch(self, as_str=True) -> Iterable:
        for key in self.get_ordered_keys():
            if as_str:
                count = self.get_count(key)
                yield f'Selection {key} ({count})'
            else:
                yield key, self.get_count(key)

    def log_selection_error(self, func, in_fields, in_values, in_record, message) -> None:
        msg = SelectionError(func, in_fields, in_values, in_record, message)
        is_new_key = self.is_new_key(msg)
        self.add_message(msg)
        if is_new_key:
            self.show_error(msg)

    def show_error(self, message: SelectionError) -> None:
        err_count = self.get_err_count()
        key_count = self.get_key_count()
        msg = message.get_str()
        msg = f'{err_count} selection errors with {key_count} keys: {msg}'
        context = self.get_context()
        if context:
            context.get_logger().show(msg, end=RETURN_CHAR)
        else:
            print(msg, end=RETURN_CHAR)

    def has_errors(self) -> bool:
        return bool(self.get_err_keys())

    def get_err_count(self) -> int:
        return sum([self.get_count(k) for k in self.get_err_keys()])


class CommonMessageCollector(SelectionMessageCollector):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(CommonMessageCollector, cls).__new__(cls)
        return cls.instance
