import loggers.extended_logger_interface

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from loggers import logger_classes as log
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from . import logger_classes as log


class BaseLogger:
    def __init__(
            self,
            name=arg.DEFAULT,
            level=arg.DEFAULT,
            formatter=arg.DEFAULT,
    ):
        name = arg.undefault(name, log.DEFAULT_LOGGER_NAME)
        level = arg.undefault(level, log.DEFAULT_LOGGING_LEVEL)
        formatter = arg.undefault(formatter, log.DEFAULT_FORMATTER)
        self.base_logger = log.get_base_logger(name, level, formatter)
        self.name = name


class ExtendedLogger(BaseLogger):
    def __init__(
            self,
            name=arg.DEFAULT,
            level=arg.DEFAULT,
            formatter=arg.DEFAULT,
            max_line_len=arg.DEFAULT,
            context=None,
    ):
        super().__init__(name=name, level=level, formatter=formatter)
        self.level = arg.undefault(level, log.DEFAULT_LOGGING_LEVEL)
        self.max_line_len = arg.undefault(max_line_len, log.DEFAULT_LINE_LEN)
        self.progress_trackers = list()
        self.context = context
        self.LoggingLevel = loggers.extended_logger_interface.LoggingLevel

    def get_context(self):
        return self.context

    def set_context(self, context):
        self.context = context

    def progress(self, items, name='Progress', count=None, step=arg.DEFAULT, context=arg.DEFAULT):
        progress = log.Progress(
            name,
            count=count,
            logger=self,
            context=arg.undefault(context, self.get_context()),
        )
        self.progress_trackers.append(progress)
        return progress.iterate(
            items,
            step=arg.undefault(step, log.DEFAULT_STEP),
        )

    def new_progress(self, name, **kwargs):
        return log.Progress(name, logger=self, **kwargs)

    def get_current_progress(self):
        for progress in reversed(self.progress_trackers):
            assert isinstance(progress, log.Progress)
            if not progress.is_finished():
                return progress

    def get_selection_logger(self, **kwargs):
        log.get_selection_logger(**kwargs)

    def log(self, msg, level=arg.DEFAULT, logger=arg.DEFAULT, end=arg.DEFAULT, verbose=True):
        level = arg.undefault(level, loggers.extended_logger_interface.LoggingLevel.Info if verbose else loggers.extended_logger_interface.LoggingLevel.Debug)
        logger = arg.undefault(logger, self.base_logger)
        if isinstance(msg, (list, tuple)):
            msg = self.format_message(*msg)
        if not isinstance(level, loggers.extended_logger_interface.LoggingLevel):
            level = loggers.extended_logger_interface.LoggingLevel(level)
        if logger:
            if level.value >= self.level:
                logging_method = getattr(logger, log.get_method_name(level))
                logging_method(msg)
        if verbose and level.value < logger.level:
            self.show(msg, end=end)

    def debug(self, msg):
        self.log(msg=msg, level=loggers.extended_logger_interface.LoggingLevel.Debug)

    def info(self, msg):
        self.log(msg=msg, level=loggers.extended_logger_interface.LoggingLevel.Info)

    def warning(self, msg):
        self.log(msg=msg, level=loggers.extended_logger_interface.LoggingLevel.Warning)

    def error(self, msg):
        self.log(msg=msg, level=loggers.extended_logger_interface.LoggingLevel.Error)

    def critical(self, msg):
        self.log(msg=msg, level=loggers.extended_logger_interface.LoggingLevel.Critical)

    def format_message(self, *messages, max_len=arg.DEFAULT):
        messages = arg.update(messages)
        max_len = arg.undefault(max_len, self.max_line_len)
        message = ' '.join([str(m) for m in messages])
        if len(message) > max_len:
            message = message[:max_len - 2] + '..'
        return message

    def clear_line(self):
        print('\r', end='')
        print(' ' * self.max_line_len, end='\r')

    def show(self, *messages, end=arg.DEFAULT, clear_before=True):
        message = self.format_message(*messages, max_len=log.LONG_LINE_LEN if end == '\n' else self.max_line_len)
        end = arg.undefault(end, '\r' if message.endswith('...') else '\n')
        if clear_before:
            remainder = self.max_line_len - len(message)
            message += ' ' * remainder
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
        self.selection_logger = None

    def get_selection_logger(self, **kwargs):
        if self.selection_logger:
            self.selection_logger.set_options(**kwargs)
        else:
            self.reset_selection_logger(**kwargs)
        return self.selection_logger

    def reset_selection_logger(self, **kwargs):
        self.selection_logger = log.CommonMessageCollector(**kwargs)
