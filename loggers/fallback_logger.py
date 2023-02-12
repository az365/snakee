from typing import Union, Optional
import warnings

try:  # Assume we're a submodule in a package.
    from base.functions.arguments import get_value
    from loggers.logger_interface import LoggerInterface, LoggingLevel
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..base.functions.arguments import get_value
    from .logger_interface import LoggerInterface, LoggingLevel

DEFAULT_LOGGER_NAME = 'fallback'
DEFAULT_LOGGING_LEVEL = 30  # LoggingLevel.get_default()


class FallbackLogger(LoggerInterface):
    def __init__(
            self,
            name: Optional[str] = None,
            ignore_warnings: bool = False,
    ):
        if name is None:
            name = DEFAULT_LOGGER_NAME
        self._name = name
        self._ignore_warnings = ignore_warnings

    def log(self, msg: str, level: Union[LoggingLevel, int, None] = None, stacklevel: int = 2, *args, **kwargs):
        if not isinstance(level, LoggingLevel):
            try:
                level = LoggingLevel(get_value(level))
            except ValueError:
                level = LoggingLevel(DEFAULT_LOGGING_LEVEL)
        if level == LoggingLevel.Debug:
            return self.debug(msg)
        elif level == LoggingLevel.Info:
            return self.info(msg)
        elif level == LoggingLevel.Warning:
            if stacklevel is not None:
                stacklevel += 1
            return self.warning(msg, stacklevel=stacklevel, **kwargs)
        elif level == LoggingLevel.Critical:
            return self.critical(msg)

    def debug(self, msg: str):
        pass

    def info(self, msg: str):
        print(f'INFO {msg}')

    def warning(self, msg: str, category: Optional[Warning] = None, stacklevel: int = 2) -> None:
        if not self._ignore_warnings:
            stacklevel += 1
            warnings.warn(msg, category=category, stacklevel=stacklevel)

    def error(self, msg: str):
        warnings.warn(f'ERROR {msg}')

    def critical(self, msg: str):
        warnings.warn(f'CRITICAL {msg}')
