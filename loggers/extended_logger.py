from typing import Union, Optional, Iterable, Any, NoReturn
import logging

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from utils.decorators import singleton
    from base import base_classes as bs
    from loggers.logger_interface import LoggerInterface
    from loggers.extended_logger_interface import ExtendedLoggerInterface, LoggingLevel
    from loggers.selection_logger_interface import SelectionLoggerInterface, SELECTION_LOGGER_NAME
    from loggers.progress_interface import ProgressInterface
    from loggers.progress import Progress
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..utils.decorators import singleton
    from ..base import base_classes as bs
    from .logger_interface import LoggerInterface
    from .extended_logger_interface import ExtendedLoggerInterface, LoggingLevel
    from .selection_logger_interface import SelectionLoggerInterface, SELECTION_LOGGER_NAME
    from .progress_interface import ProgressInterface
    from .progress import Progress

Level = Union[LoggingLevel, int, arg.DefaultArgument]
Name = str
Count = Optional[int]
Step = Union[Count, arg.DefaultArgument]
Formatter = Union[str, logging.Formatter]
Context = Optional[bs.ContextInterface]
OptContext = Union[Context, arg.DefaultArgument]
OptName = Union[Name, arg.DefaultArgument]
BaseLogger = Union[LoggerInterface, Any]
SubLoggers = Optional[Union[list, dict]]
File = bs.AbstractNamed
FileOrName = Union[File, Name]

DEFAULT_LOGGER_NAME = 'default'
DEFAULT_FORMATTER = '%(asctime)s - %(levelname)s - %(message)s'
DEFAULT_LOGGING_LEVEL = LoggingLevel.get_default()
DEFAULT_ENCODING = 'utf8'
DEFAULT_LINE_LEN = 127
LONG_LINE_LEN = 600
TRUNCATED_SUFFIX = '..'
REWRITE_SUFFIX = '...'
SPACE = ' '


class BaseLoggerWrapper(bs.TreeItem, LoggerInterface):
    def __init__(
            self,
            name: OptName = arg.DEFAULT,
            level: Level = arg.DEFAULT,
            formatter: Union[Formatter, arg.DefaultArgument] = arg.DEFAULT,
            loggers: SubLoggers = arg.DEFAULT,
            context: Context = None,
            file: Optional[FileOrName] = None,
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
            level_value = arg.get_value(level)
            base_logger = self.build_base_logger(name, level_value, formatter)
            loggers[name] = base_logger
        self._level = level
        super().__init__(name=name, children=loggers, context=context)
        if file:
            self.set_file(file)

    @staticmethod
    def build_base_logger(
            name: Name = DEFAULT_LOGGER_NAME,
            level: Level = logging.DEBUG,
            formatter: Formatter = DEFAULT_FORMATTER,
    ) -> BaseLogger:
        level = arg.get_value(level)
        base_logger = logging.getLogger(name)
        base_logger.setLevel(level)
        if not base_logger.handlers:
            stream_handler = logging.StreamHandler()
            stream_handler.setLevel(level)
            if isinstance(formatter, str):
                formatter = logging.Formatter(formatter)
            stream_handler.setFormatter(formatter)
            base_logger.addHandler(stream_handler)
        return base_logger

    def get_base_logger(self) -> BaseLogger:
        return self.get_child(self.get_name())

    def get_level(self) -> LoggingLevel:
        return self._level

    def get_handlers(self) -> list:
        return self.get_base_logger().handlers

    def add_handler(self, handler: logging.Handler, if_not_added: bool = True) -> LoggerInterface:
        if handler not in self.get_handlers() or not if_not_added:
            self.get_base_logger().addHandler(handler)
        return self

    def set_file(
            self, file: Union[File, Name],
            encoding: str = DEFAULT_ENCODING,
            level: Level = DEFAULT_LOGGING_LEVEL,
            formatter: Formatter = DEFAULT_FORMATTER,
            if_not_added: bool = True,
    ) -> LoggerInterface:
        filename = arg.get_name(file)
        level = arg.get_value(level)
        is_added = False
        for h in self.get_handlers():
            if hasattr(h, 'baseFilename'):
                if h.baseFilename.endswidth(filename):
                    is_added = True
                    break
        if not (is_added and if_not_added):
            file_handler = logging.FileHandler(filename, encoding=encoding)
            file_handler.setLevel(level)
            if isinstance(formatter, str):
                formatter = logging.Formatter(formatter)
            file_handler.setFormatter(formatter)
            self.add_handler(file_handler)
        return self

    def log(self, msg: str, level: Level, *args, **kwargs) -> NoReturn:
        return self.get_base_logger().log(level=level, msg=msg, *args, **kwargs)

    def debug(self, msg: str) -> NoReturn:
        self.log(msg=msg, level=LoggingLevel.Debug)

    def info(self, msg: str) -> NoReturn:
        self.log(msg=msg, level=LoggingLevel.Info)

    def warning(self, msg: str) -> NoReturn:
        self.log(msg=msg, level=LoggingLevel.Warning)

    def error(self, msg: str) -> NoReturn:
        self.log(msg=msg, level=LoggingLevel.Error)

    def critical(self, msg: str) -> NoReturn:
        self.log(msg=msg, level=LoggingLevel.Critical)


class ExtendedLogger(BaseLoggerWrapper, ExtendedLoggerInterface):
    def __init__(
            self,
            name: Union[Name, arg.DefaultArgument] = arg.DEFAULT,
            level: Level = arg.DEFAULT, formatter=arg.DEFAULT,
            max_line_len=arg.DEFAULT,
            context: Context = None,
            file: Optional[FileOrName] = None,
    ):
        self.max_line_len = arg.undefault(max_line_len, DEFAULT_LINE_LEN)
        progress_trackers = dict()
        self.LoggingLevel = LoggingLevel
        super().__init__(
            name=name, level=level, formatter=formatter,
            file=file, loggers=progress_trackers,
            context=context,
        )

    @staticmethod
    def is_common_logger() -> bool:
        return False

    def put_into_context(self, check: bool = False) -> NoReturn:
        context = self.get_context()
        if context:
            if check:
                registered_logger = context.get_logger(create_if_not_yet=False)
                if registered_logger:
                    if hasattr(registered_logger, 'get_key_member_values'):
                        is_same_logger = registered_logger.get_key_member_values() == self.get_key_member_values()
                        assert is_same_logger, 'Context already has logger registered'
            context.set_logger(self)

    def get_new_progress(self, name: Name, count: Count = None, context: OptContext = arg.DEFAULT) -> ProgressInterface:
        progress = Progress(
            name=name,
            count=count,
            logger=self,
            context=arg.undefault(context, self.get_context, delayed=True),
        )
        self.add_child(progress, check=False)
        return progress

    def progress(
            self, items: Iterable, name: Name = 'Progress',
            count: Count = None, step: Step = arg.DEFAULT,
            context: OptContext = arg.DEFAULT,
    ) -> Iterable:
        return self.get_new_progress(name, count=count, context=context).iterate(items, step=step)

    def get_selection_logger(self, name: OptName = arg.DEFAULT, **kwargs) -> Optional[SelectionLoggerInterface]:
        name = arg.undefault(name, SELECTION_LOGGER_NAME)
        selection_logger = self.get_child(name)
        if selection_logger:
            if kwargs:
                selection_logger.set_meta(name, **kwargs)
        else:
            selection_logger = self.reset_selection_logger(name, **kwargs)
        return selection_logger

    def set_selection_logger(self, selection_logger: SelectionLoggerInterface, skip_errors: bool = True) -> NoReturn:
        try:
            assert isinstance(selection_logger, bs.ContextualDataWrapper)
            self.add_child(selection_logger)
        except ValueError as e:
            if not skip_errors:
                raise e

    def reset_selection_logger(self, name: OptName = arg.DEFAULT, **kwargs) -> Optional[SelectionLoggerInterface]:
        name = arg.undefault(name, SELECTION_LOGGER_NAME)
        context = self.get_context()
        if context:
            selection_logger = context.get_new_selection_logger(name, **kwargs)
            if selection_logger:
                self.set_selection_logger(selection_logger)
                return selection_logger

    def is_suitable_level(self, level: Level) -> bool:
        proposed_level_value = arg.get_value(level)
        selected_level_value = arg.get_value(self.get_level())
        return proposed_level_value >= selected_level_value

    def log(
            self,
            msg: Union[str, list, tuple], level: Level = arg.DEFAULT,
            logger: Union[BaseLogger, arg.DefaultArgument] = arg.DEFAULT,
            end: Union[str, arg.DefaultArgument] = arg.DEFAULT,
            verbose: bool = True, truncate: bool = True,
    ) -> NoReturn:
        level = arg.undefault(level, LoggingLevel.Info if verbose else LoggingLevel.Debug)
        logger = arg.delayed_undefault(logger, self.get_base_logger)
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

    def format_message(
            self, *messages,
            max_len: Union[int, arg.DefaultArgument] = arg.DEFAULT,
            truncate: bool = True,
    ) -> str:
        messages = arg.update(messages)
        max_len = arg.undefault(max_len, self.max_line_len)
        message = SPACE.join([str(m) for m in messages])
        if truncate and len(message) > max_len:
            message = message[:max_len - 2] + TRUNCATED_SUFFIX
        return message

    def clear_line(self) -> NoReturn:
        print('\r', end='')
        print(SPACE * self.max_line_len, end='\r')

    def show(
            self, *messages,
            end: Union[str, arg.DefaultArgument] = arg.DEFAULT,
            clear_before: bool = True, truncate: bool = True,
    ) -> NoReturn:
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


@singleton
class SingletonLogger(ExtendedLogger):
    @staticmethod
    def is_common_logger() -> bool:
        return True
