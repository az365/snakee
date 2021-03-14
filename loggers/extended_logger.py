from typing import Union, Optional, Iterable, Any
import logging

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base import base_classes as bs
    from loggers.logger_interface import LoggerInterface
    from loggers.extended_logger_interface import ExtendedLoggerInterface, LoggingLevel
    from loggers.progress_interface import ProgressInterface
    from loggers.progress import Progress
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..base import base_classes as bs
    from .logger_interface import LoggerInterface
    from .extended_logger_interface import ExtendedLoggerInterface, LoggingLevel
    from .progress_interface import ProgressInterface
    from .progress import Progress

Level = Union[LoggingLevel, arg.DefaultArgument]
Context = Optional[bs.ContextInterface]
BaseLogger = Union[LoggerInterface, Any]
SubLoggers = Optional[Union[list, dict]]
SelectionLogger = Union[bs.AbstractNamed, bs.DataWrapper, Any]

SELECTION_LOGGER_NAME = 'SelectorMessageCollector'
DEFAULT_LOGGER_NAME = 'default'
DEFAULT_FORMATTER = '%(asctime)s - %(levelname)s - %(message)s'
DEFAULT_LOGGING_LEVEL = LoggingLevel.get_default()
DEFAULT_LINE_LEN = 127
LONG_LINE_LEN = 600
TRUNCATED_SUFFIX = '..'
REWRITE_SUFFIX = '...'
SPACE = ' '


class BaseLoggerWrapper(bs.TreeItem, LoggerInterface):
    def __init__(
            self,
            name: Union[str, arg.DefaultArgument] = arg.DEFAULT,
            level: Level = arg.DEFAULT, formatter=arg.DEFAULT,
            loggers: SubLoggers = arg.DEFAULT,
            context: Context = None,
    ):
        name = arg.undefault(name, DEFAULT_LOGGER_NAME)
        level = arg.undefault(level, DEFAULT_LOGGING_LEVEL)
        formatter = arg.undefault(formatter, DEFAULT_FORMATTER)
        if not isinstance(level, LoggingLevel):
            level = LoggingLevel(level)
        if isinstance(loggers, list):
            loggers = {i: i.get_name() for i in loggers}
        elif not arg.is_defined(loggers):
            loggers = dict()
        if name not in loggers:
            level_value = level.get_value()
            base_logger = self.build_base_logger(name, level_value, formatter)
            loggers[name] = base_logger
        self._level = level
        super().__init__(name=name, children=loggers, context=context)

    @staticmethod
    def build_base_logger(name=DEFAULT_LOGGER_NAME, level=DEFAULT_LOGGING_LEVEL, formatter=DEFAULT_FORMATTER):
        if isinstance(level, LoggingLevel):
            level = level.get_value()
        base_logger = logging.getLogger(name)
        base_logger.setLevel(level)
        if not base_logger.handlers:
            stream_handler = logging.StreamHandler()
            stream_handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter(formatter)
            stream_handler.setFormatter(formatter)
            base_logger.addHandler(stream_handler)
        return base_logger

    def get_base_logger(self) -> BaseLogger:
        return self.get_child(self.get_name())

    def get_level(self) -> LoggingLevel:
        return self._level

    def log(self, msg, level, *args, **kwargs):
        return self.get_base_logger().log(level=level, msg=msg, *args, **kwargs)

    def debug(self, msg):
        self.log(msg=msg, level=LoggingLevel.Debug)

    def info(self, msg):
        self.log(msg=msg, level=LoggingLevel.Info)

    def warning(self, msg):
        self.log(msg=msg, level=LoggingLevel.Warning)

    def error(self, msg):
        self.log(msg=msg, level=LoggingLevel.Error)

    def critical(self, msg):
        self.log(msg=msg, level=LoggingLevel.Critical)


class ExtendedLogger(BaseLoggerWrapper, ExtendedLoggerInterface):
    def __init__(
            self,
            name: Union[str, arg.DefaultArgument] = arg.DEFAULT,
            level: Level = arg.DEFAULT, formatter=arg.DEFAULT,
            max_line_len=arg.DEFAULT,
            context: Context = None,
    ):
        self.max_line_len = arg.undefault(max_line_len, DEFAULT_LINE_LEN)
        progress_trackers = dict()
        self.LoggingLevel = LoggingLevel
        super().__init__(name=name, level=level, formatter=formatter, loggers=progress_trackers, context=context)

    @staticmethod
    def is_common_logger() -> bool:
        return False

    def put_into_context(self, check=False):
        context = self.get_context()
        if context:
            if check:
                assert not context.get_logger(create_if_not_yet=False), 'Context already has logger registered'
            context.set_logger(self)

    def get_new_progress(self, name, count=None, context=arg.DEFAULT) -> ProgressInterface:
        progress = Progress(
            name=name,
            count=count,
            logger=self,
            context=arg.undefault(context, self.get_context, delayed=True),
        )
        self.add_child(progress)
        return progress

    def progress(self, items: Iterable, name='Progress', count=None, step=arg.DEFAULT, context=arg.DEFAULT) -> Iterable:
        return self.get_new_progress(name, count=count, context=context).iterate(items, step=step)

    def get_selection_logger(self, name=arg.DEFAULT, **kwargs):
        name = arg.undefault(name, SELECTION_LOGGER_NAME)
        selection_logger = self.get_child(name)
        if selection_logger:
            if kwargs:
                selection_logger.set_meta(name, **kwargs)
        else:
            self.reset_selection_logger(name, **kwargs)
        return selection_logger

    def set_selection_logger(self, selection_logger: SelectionLogger, skip_errors=True):
        try:
            self.add_child(selection_logger)
        except ValueError as e:
            if not skip_errors:
                raise e

    def reset_selection_logger(self, name: Union[str, arg.DefaultArgument] = arg.DEFAULT, **kwargs):
        name = arg.undefault(name, SELECTION_LOGGER_NAME)
        context = self.get_context()
        if context:
            selection_logger = context.get_new_selection_logger(name, **kwargs)
            if selection_logger:
                self.set_selection_logger(selection_logger)

    def is_suitable_level(self, level: Union[int, LoggingLevel]) -> bool:
        if hasattr(level, 'get_value'):
            proposed_level_value = level.get_value()
        else:
            proposed_level_value = level
        if hasattr(self.get_level(), 'get_value'):
            selected_level_value = self.get_level().get_value()
        else:
            selected_level_value = self.get_level()
        return proposed_level_value >= selected_level_value

    def log(self, msg, level=arg.DEFAULT, logger=arg.DEFAULT, end=arg.DEFAULT, verbose=True, truncate=True):
        level = arg.undefault(level, LoggingLevel.Info if verbose else LoggingLevel.Debug)
        logger = arg.undefault(logger, self.get_base_logger())
        if isinstance(msg, (list, tuple)):
            msg = self.format_message(*msg)
        if not isinstance(level, LoggingLevel):
            level = LoggingLevel(level)
        if logger:
            if self.is_suitable_level(level):
                logging_method = getattr(logger, level.get_method_name())
                logging_method(msg)
        if verbose and not self.is_suitable_level(level):
            self.show(msg, end=end, truncate=truncate)

    def format_message(self, *messages, max_len=arg.DEFAULT, truncate=True):
        messages = arg.update(messages)
        max_len = arg.undefault(max_len, self.max_line_len)
        message = SPACE.join([str(m) for m in messages])
        if truncate and len(message) > max_len:
            message = message[:max_len - 2] + TRUNCATED_SUFFIX
        return message

    def clear_line(self):
        print('\r', end='')
        print(SPACE * self.max_line_len, end='\r')

    def show(self, *messages, end=arg.DEFAULT, clear_before=True, truncate=True):
        message = self.format_message(
            *messages,
            max_len=LONG_LINE_LEN if end == '\n' else self.max_line_len,
            truncate=truncate,
        )
        end = arg.undefault(end, '\r' if message.endswith(REWRITE_SUFFIX) else '\n')
        if clear_before:
            remainder = self.max_line_len - len(message)
            message += SPACE * remainder
        print(message, end=end)


class SingletonLogger(ExtendedLogger):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(SingletonLogger, cls).__new__(cls)
        return cls.instance

    def __init__(
            self,
            name=arg.DEFAULT,
            level=arg.DEFAULT,
            formatter=arg.DEFAULT,
            max_line_len=arg.DEFAULT,
            context=None,
    ):
        super().__init__(name, level, formatter, max_line_len, context)

    @staticmethod
    def is_common_logger() -> bool:
        return True
