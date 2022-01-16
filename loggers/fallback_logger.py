from typing import Union, Optional
import warnings

try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from loggers.logger_interface import LoggerInterface, LoggingLevel
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from .logger_interface import LoggerInterface, LoggingLevel

DEFAULT_LOGGER_NAME = 'fallback'
DEFAULT_LOGGING_LEVEL = 30  # LoggingLevel.get_default()


class FallbackLogger(LoggerInterface):
    def __init__(
            self,
            name: Union[str, arg.Auto] = arg.AUTO,
            ignore_warnings: bool = False,
    ):
        self._name = arg.acquire(name, DEFAULT_LOGGER_NAME)
        self._ignore_warnings = ignore_warnings

    def log(self, msg, level: Union[LoggingLevel, int] = DEFAULT_LOGGING_LEVEL, stacklevel: int = 2, *args, **kwargs):
        if not level:
            level = LoggingLevel(level)
        else:
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

    def debug(self, msg):
        pass

    def info(self, msg):
        print('INFO {}'.format(msg))

    def warning(self, msg, category: Optional[Warning] = None, stacklevel: int = 2):
        if not self._ignore_warnings:
            stacklevel += 1
            warnings.warn(msg, category=category, stacklevel=stacklevel)

    def error(self, msg):
        warnings.warn('ERROR {}'.format(msg))

    def critical(self, msg):
        warnings.warn('CRITICAL {}'.format(msg))
