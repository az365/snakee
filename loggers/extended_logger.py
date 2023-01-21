from typing import Union, Optional, Iterable, Generator, Type, Any
from inspect import getframeinfo, stack
import logging

try:  # Assume we're a submodule in a package.
    from base.classes.typing import AUTO, Auto, AutoName, AutoCount, Count, Name
    from base.constants.chars import DEFAULT_ENCODING, DEFAULT_LINE_LEN, LONG_LINE_LEN, CROP_SUFFIX, ELLIPSIS, SPACE
    from base.functions.arguments import get_name, get_value, update, get_cropped_text
    from base.interfaces.context_interface import ContextInterface
    from base.abstract.named import AbstractNamed
    from base.abstract.tree_item import TreeItem
    from utils.decorators import singleton
    from loggers.logger_interface import LoggerInterface
    from loggers.extended_logger_interface import ExtendedLoggerInterface, LoggingLevel
    from loggers.selection_logger_interface import SelectionLoggerInterface, SELECTION_LOGGER_NAME
    from loggers.progress_interface import ProgressInterface
    from loggers.progress import Progress
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..base.classes.typing import AUTO, Auto, AutoName, AutoCount, Count, Name
    from ..base.constants.chars import DEFAULT_ENCODING, DEFAULT_LINE_LEN, LONG_LINE_LEN, CROP_SUFFIX, ELLIPSIS, SPACE
    from ..base.functions.arguments import get_name, get_value, update, get_cropped_text
    from ..base.interfaces.context_interface import ContextInterface
    from ..base.abstract.named import AbstractNamed
    from ..base.abstract.tree_item import TreeItem
    from ..utils.decorators import singleton
    from .logger_interface import LoggerInterface
    from .extended_logger_interface import ExtendedLoggerInterface, LoggingLevel
    from .selection_logger_interface import SelectionLoggerInterface, SELECTION_LOGGER_NAME
    from .progress_interface import ProgressInterface
    from .progress import Progress

Native = Union[TreeItem, LoggerInterface]
Level = Union[LoggingLevel, int, Auto]
Formatter = Union[str, logging.Formatter]
Context = Optional[ContextInterface]
OptContext = Union[Context, Auto]
BaseLogger = Union[LoggerInterface, Any]
SubLoggers = Optional[Union[list, dict]]
File = AbstractNamed

DEFAULT_LOGGER_NAME = 'default'
DEFAULT_FORMATTER = '%(asctime)s - %(levelname)s - %(message)s'
DEFAULT_LOGGING_LEVEL = LoggingLevel.get_default()
REWRITE_SUFFIX = ELLIPSIS


class BaseLoggerWrapper(TreeItem, LoggerInterface):
    def __init__(
            self,
            name: AutoName = AUTO,
            level: Level = AUTO,
            formatter: Union[Formatter, Auto] = AUTO,
            loggers: SubLoggers = AUTO,
            context: Context = None,
            file: Union[File, Name, None] = None,
    ):
        name = Auto.acquire(name, DEFAULT_LOGGER_NAME)
        level = Auto.acquire(level, DEFAULT_LOGGING_LEVEL)
        formatter = Auto.acquire(formatter, DEFAULT_FORMATTER)
        if not isinstance(level, LoggingLevel):
            level = LoggingLevel(level)
        if isinstance(loggers, list):
            loggers = {i: i.get_name() for i in loggers}
        elif not Auto.is_defined(loggers):
            loggers = dict()
        if name not in loggers:
            level_value = get_value(level)
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
        level = get_value(level)
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
            self,
            file: Union[File, Name],
            encoding: str = DEFAULT_ENCODING,
            level: Level = DEFAULT_LOGGING_LEVEL,
            formatter: Formatter = DEFAULT_FORMATTER,
            if_not_added: bool = True,
    ) -> LoggerInterface:
        filename = get_name(file)
        level = get_value(level)
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

    def log(self, msg: str, level: Level, *args, **kwargs) -> None:
        return self.get_base_logger().log(level=level, msg=msg, *args, **kwargs)

    def debug(self, msg: str) -> None:
        self.log(msg=msg, level=LoggingLevel.Debug)

    def info(self, msg: str) -> None:
        self.log(msg=msg, level=LoggingLevel.Info)

    def warning(self, msg: str, category: Optional[Type] = None, stacklevel: Optional[int] = None) -> None:
        if stacklevel:
            caller = getframeinfo(stack()[stacklevel][0])
            category_name = get_name(category) if category else ''
            msg = '{}:{}: {} {}'.format(caller.filename, caller.lineno, category_name, msg)
        self.log(msg=msg, level=LoggingLevel.Warning)

    def error(self, msg: str) -> None:
        self.log(msg=msg, level=LoggingLevel.Error)

    def critical(self, msg: str) -> None:
        self.log(msg=msg, level=LoggingLevel.Critical)


class ExtendedLogger(BaseLoggerWrapper, ExtendedLoggerInterface):
    def __init__(
            self,
            name: Union[Name, Auto] = AUTO,
            level: Level = AUTO,
            formatter: Union[Formatter, Auto] = AUTO,
            max_line_len=AUTO,
            context: Context = None,
            file: Union[File, Name, None] = None,
    ):
        self.max_line_len = Auto.acquire(max_line_len, DEFAULT_LINE_LEN)
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

    def put_into_context(self, check: bool = False) -> None:
        context = self.get_context()
        if context:
            if check:
                registered_logger = context.get_logger(create_if_not_yet=False)
                if registered_logger:
                    if hasattr(registered_logger, 'get_key_member_values'):
                        is_same_logger = registered_logger.get_key_member_values() == self.get_key_member_values()
                        assert is_same_logger, 'Context already has logger registered'
            context.set_logger(self)

    def get_new_progress(self, name: Name, count: Count = None, context: OptContext = AUTO) -> ProgressInterface:
        progress = Progress(
            name=name,
            count=count,
            logger=self,
            context=Auto.acquire(context, self.get_context, delayed=True),
        )
        self.add_child(progress, check=False)
        return progress

    def progress(
            self,
            items: Iterable,
            name: Name = 'Progress',
            count: Count = None,
            step: AutoCount = AUTO,
            context: OptContext = AUTO,
    ) -> Generator:
        return self.get_new_progress(name, count=count, context=context).iterate(items, step=step)

    def get_selection_logger(self, name: AutoName = AUTO, **kwargs) -> Optional[SelectionLoggerInterface]:
        name = Auto.acquire(name, SELECTION_LOGGER_NAME)
        selection_logger = self.get_child(name)
        if selection_logger:
            if kwargs:
                selection_logger.set_meta(name, **kwargs)
        else:
            selection_logger = self.reset_selection_logger(name, **kwargs)
        return selection_logger

    def set_selection_logger(self, selection_logger: SelectionLoggerInterface, skip_errors: bool = True) -> Native:
        try:
            self.add_child(selection_logger)
            return self
        except ValueError as e:
            if not skip_errors:
                raise ValueError('{obj}: {e}'.format(obj=self, e=e))

    def reset_selection_logger(self, name: AutoName = AUTO, **kwargs) -> Optional[SelectionLoggerInterface]:
        name = Auto.acquire(name, SELECTION_LOGGER_NAME)
        context = self.get_context()
        if context:
            selection_logger = context.get_new_selection_logger(name, **kwargs)
            if selection_logger:
                self.set_selection_logger(selection_logger)
                return selection_logger

    def is_suitable_level(self, level: Level) -> bool:
        proposed_level_value = get_value(level)
        selected_level_value = get_value(self.get_level())
        return proposed_level_value >= selected_level_value

    def log(
            self,
            msg: Union[str, list, tuple],
            level: Level = AUTO,
            logger: Union[BaseLogger, Auto] = AUTO,
            end: Union[str, Auto] = AUTO,
            verbose: bool = True,
            truncate: bool = True,
            category: Optional[Type] = None,
            stacklevel: Optional[int] = None,
    ) -> None:
        level = Auto.acquire(level, LoggingLevel.Info if verbose else LoggingLevel.Debug)
        logger = Auto.delayed_acquire(logger, self.get_base_logger)
        if isinstance(msg, BaseException):
            msg = str(msg)
        if isinstance(msg, str):
            msg = [msg]
        elif isinstance(msg, Iterable):
            msg = list(msg)
        else:
            raise TypeError('Expected msg as str or list[str], got {}'.format(msg))
        if category:
            category_name = get_name(category)
            msg = [category_name] + msg
        if Auto.is_defined(stacklevel):
            caller = getframeinfo(stack()[stacklevel + 1][0])
            file_name_without_path = caller.filename.split('\\')[-1].split('/')[-1]
            msg = ['{}:{}:'.format(file_name_without_path, caller.lineno)] + msg
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
            max_len: Union[int, Auto] = AUTO,
            truncate: bool = True,
    ) -> str:
        messages = update(messages)
        max_len = Auto.acquire(max_len, self.max_line_len)
        message = SPACE.join([str(m) for m in messages])
        if truncate:
            message = get_cropped_text(message, max_len=max_len)
        return message

    def clear_line(self) -> None:
        print('\r', end='')
        print(SPACE * self.max_line_len, end='\r')

    def show(
            self, *messages,
            end: Union[str, Auto] = AUTO,
            clear_before: bool = True,
            truncate: bool = True,
    ) -> None:
        message = self.format_message(
            *messages,
            max_len=LONG_LINE_LEN if end == '\n' else self.max_line_len,
            truncate=truncate,
        )
        end = Auto.acquire(end, '\r' if message.endswith(REWRITE_SUFFIX) else '\n')
        if clear_before:
            remainder = self.max_line_len - len(message)
            message += SPACE * remainder
        print(message, end=end)


@singleton
class SingletonLogger(ExtendedLogger):
    @staticmethod
    def is_common_logger() -> bool:
        return True
