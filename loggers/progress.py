from typing import Optional, Union, Generator
from datetime import timedelta, datetime

try:  # Assume we're a submodule in a package.
    from base.classes.auto import AUTO, Auto
    from base.interfaces.context_interface import ContextInterface
    from base.abstract.tree_item import TreeItem
    from loggers.extended_logger_interface import ExtendedLoggerInterface, LoggingLevel
    from loggers.progress_interface import ProgressInterface, OperationStatus
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from base.classes.auto import AUTO, Auto
    from ..base.interfaces.context_interface import ContextInterface
    from ..base.abstract.tree_item import TreeItem
    from .extended_logger_interface import ExtendedLoggerInterface, LoggingLevel
    from .progress_interface import ProgressInterface, OperationStatus

Logger = Union[ExtendedLoggerInterface, Auto]
Context = Optional[ContextInterface]

DEFAULT_STEP = 10000


class Progress(TreeItem, ProgressInterface):
    def __init__(
            self,
            name: str = 'Progress',
            count: Optional[int] = None,
            timing: bool = True,
            verbose: bool = True,
            logger: Logger = AUTO,
            context: ContextInterface = None,
    ):
        self.expected_count = count
        self.verbose = verbose
        self.state = OperationStatus.New
        self.position = 0
        self.timing = timing
        self.start_time = None
        self.past_time = timedelta(0)
        if logger is None:
            logger = None
        elif logger == AUTO:
            logger = context.get_logger()
        else:
            logger = logger
        if hasattr(logger, 'get_context') and not context:
            context = logger.get_context()
        super().__init__(name=name, parent=logger, context=context, check=False)

    @staticmethod
    def is_progress() -> bool:
        return True

    def get_position(self) -> int:
        return self.position

    def set_position(self, position: int, inplace: bool = True) -> Optional[ProgressInterface]:
        self.position = position
        if not inplace:
            return self

    def _get_selection_logger_name(self) -> str:
        return self.get_name() + ':_selection'

    def get_selection_logger(self, name=AUTO):
        name = Auto.acquire(name, self._get_selection_logger_name, delayed=True)
        selection_logger = self.get_child(name)
        if not selection_logger:
            logger = self.get_logger()
            if hasattr(logger, 'get_selection_logger'):
                selection_logger = logger.get_selection_logger(name=name)
        if not selection_logger:
            context = self.get_context()
            if context:
                selection_logger = context.get_selection_logger(name=name)
            if not selection_logger:
                logger = self.get_logger()
                if hasattr(logger, 'get_selection_logger'):
                    selection_logger = logger.get_selection_logger(name=name)
        if selection_logger:
            self.add_child(selection_logger)
        return selection_logger

    def reset_selection_logger(self, name=AUTO, **kwargs) -> None:
        logger = self.get_logger()
        logger.reset_selection_logger(name, **kwargs)

    def get_logger(self) -> ExtendedLoggerInterface:
        logger = self.get_parent()
        assert isinstance(logger, ExtendedLoggerInterface)
        return logger

    def log(self, msg, level=AUTO, end=AUTO, verbose=AUTO) -> None:
        logger = self.get_logger()
        if logger is not None:
            logger.log(
                logger=self.get_logger(),
                msg=msg, level=level, end=end,
                verbose=Auto.acquire(verbose, self.verbose),
            )

    def log_selection_batch(self, level=AUTO, reset_after=True) -> None:
        selection_logger = self.get_selection_logger()
        if selection_logger:
            if selection_logger.has_errors():
                batch = selection_logger.get_message_batch()
                for msg in batch:
                    self.log(msg, level=level)
            if reset_after:
                self.reset_selection_logger(selection_logger.get_name())

    def is_started(self) -> bool:
        return self.start_time is not None

    def is_finished(self) -> bool:
        if self.expected_count:
            return self.position >= self.expected_count

    def get_percent(self, round_digits=1, default_value='UNK') -> str:
        share = self.evaluate_share()
        if share is None or share > 1:
            return default_value
        else:
            share = round(100 * share, round_digits)
            return '{}%'.format(share)

    def evaluate_share(self) -> float:
        if self.expected_count:
            return (self.position + 1) / self.expected_count

    def evaluate_speed(self) -> int:
        cur_time = datetime.now()
        self.past_time = cur_time - self.start_time
        if self.past_time.total_seconds():
            speed = self.position / self.past_time.total_seconds()
            return int(speed)

    def get_timing_str(self) -> str:
        if self.is_finished():
            finish_time = datetime.now()
            self.past_time = finish_time - self.start_time
            past_minutes = int(self.past_time.total_seconds() / 60)
            return "{:02}:{:02}+{:02}:{:02}'{:02}={:02}:{:02}".format(
                self.start_time.hour, self.start_time.minute,
                int(past_minutes / 60), past_minutes % 60, int(self.past_time.total_seconds()) % 60,
                finish_time.hour, finish_time.minute,
            )
        elif self.expected_count:
            finish_time = self.start_time + self.past_time / (self.evaluate_share() or 0.001)
            rest_time = finish_time - datetime.now()
            has_long_rest_time = rest_time.total_seconds() >= 100 * 60
            if has_long_rest_time:
                return "{:02}:{:02}+{:02}+{:03}~{:02}:{:02}".format(
                    self.start_time.hour, self.start_time.minute,
                    int(round(self.past_time.total_seconds() / 60, 0)), int(rest_time.total_seconds() / 60),
                    finish_time.hour, finish_time.minute,
                )
            else:
                return "{:02}:{:02}+{:02}+{:02}'{:02}~{:02}:{:02}".format(
                    self.start_time.hour, self.start_time.minute,
                    int(round(self.past_time.total_seconds() / 60, 0)),
                    int(rest_time.total_seconds() / 60), int(rest_time.total_seconds()) % 60,
                    finish_time.hour, finish_time.minute,
                )
        else:
            return "{:02}:{:02}+{:02}'{:02}".format(
                self.start_time.hour, self.start_time.minute,
                int(self.past_time.total_seconds() / 60), int(round(self.past_time.total_seconds() % 60, 0)),
            )

    def update_now(self, cur):
        self.position = cur or self.position or 0
        if self.state != OperationStatus.InProgress:
            self.start(cur)
        if self.expected_count:
            line = '{name}: {percent} ({pos}/{count})'.format(
                name=self.get_name(),
                percent=self.get_percent(),
                pos=self.position + 1,
                count=self.expected_count,
            )
        else:
            line = '{}: {} items processed'.format(self.get_name(), self.position + 1)
        selection_logger = self.get_selection_logger()
        if selection_logger:
            line = '{}, {} err'.format(line, selection_logger.get_err_count())
        if self.timing:
            line = '{} {} ({} it/sec)'.format(self.get_timing_str(), line, self.evaluate_speed())
        self.log(line, level=LoggingLevel.Debug, end='\r')

    def update_with_step(self, position, step=AUTO):
        step = Auto.acquire(step, DEFAULT_STEP)
        cur_increment = position - (self.position or 0)
        self.position = position
        step_passed = (self.position + 1) % step == 0
        step_passed = step_passed or (cur_increment >= step)
        expected_count = self.expected_count
        if not Auto.is_defined(expected_count):
            expected_count = 0
        pool_finished = 0 < expected_count < (self.position + 1)
        if step_passed or pool_finished:
            self.update_now(position)

    def update(self, position, step=None, message=None):
        if Auto.is_defined(message):
            self.set_name(message, inplace=True)
        if step == 1 or not Auto.is_defined(step):
            self.update_now(position)
        else:
            self.update_with_step(position, step)

    def start(self, position=0):
        self.state = OperationStatus.InProgress
        self.start_time = datetime.now()
        self.position = position or self.position or 0
        if self.position != position:
            self.update(position)
        elif self.verbose:
            self.log('{} ({} items): starting...'.format(self.get_name(), self.expected_count))

    def finish(self, position=None, log_selection_batch=True):
        self.expected_count = position
        self.update(position)
        message = '{}: Done. {} items processed'.format(self.get_name(), self.position + 1)
        if self.timing:
            message = '{} {} ({} it/sec)'.format(self.get_timing_str(), message, self.evaluate_speed())
        self.log(message)
        if log_selection_batch:
            self.log_selection_batch()

    def iterate(self, items, name=None, expected_count=None, step=AUTO, log_selection_batch=True) -> Generator:
        if Auto.is_defined(name):
            self.set_name(name, inplace=True)
        if isinstance(items, (set, list, tuple)):
            self.expected_count = len(items)
        else:
            self.expected_count = expected_count or self.expected_count
        n = 0
        step = Auto.acquire(step, DEFAULT_STEP)
        self.start()
        for n, item in enumerate(items):
            self.update(n, step)
            yield item
        self.finish(n, log_selection_batch=log_selection_batch)
