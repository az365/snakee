from typing import Optional, Iterable, Generator, Union
from datetime import timedelta, datetime

try:  # Assume we're a submodule in a package.
    from base.classes.auto import AUTO, Auto
    from base.interfaces.context_interface import ContextInterface
    from base.abstract.tree_item import TreeItem
    from loggers.extended_logger_interface import ExtendedLoggerInterface, LoggingLevel
    from loggers.progress_interface import ProgressInterface, OperationStatus
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..base.classes.auto import AUTO, Auto
    from ..base.interfaces.context_interface import ContextInterface
    from ..base.abstract.tree_item import TreeItem
    from .extended_logger_interface import ExtendedLoggerInterface, LoggingLevel
    from .progress_interface import ProgressInterface, OperationStatus

Native = ProgressInterface
Logger = Union[ExtendedLoggerInterface, Auto]
Context = Optional[ContextInterface]

DEFAULT_STEP = 10000
SMALL_SHARE = 0.001
SECONDS_IN_MINUTE = 60
MINUTES_IN_HOUR = 60


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
        self._expected_count = count
        self._verbose = verbose
        self._state = OperationStatus.New
        self._position = 0
        self._timing = timing
        self._start_time = None
        self._past_time = timedelta(0)
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

    def is_verbose(self) -> bool:
        return self._verbose

    def has_timing(self) -> bool:
        return self._timing

    def get_state(self) -> OperationStatus:
        return self._state

    def set_state(self, state: OperationStatus):
        self._state = state

    state = property(get_state, set_state)

    def get_position(self) -> int:
        return self._position

    def set_position(self, position: int):
        self._position = position

    position = property(get_position, set_position)

    def get_expected_count(self) -> Optional[int]:
        return self._expected_count

    def set_expected_count(self, count: int):
        self._expected_count = count

    expected_count = property(get_expected_count, set_expected_count)

    def get_start_time(self) -> Optional[datetime]:
        return self._start_time

    def set_start_time(self, start_time: datetime):
        self._start_time = start_time

    start_time = property(get_start_time, set_start_time)

    def get_past_time(self) -> Optional[timedelta]:
        return self._past_time

    def set_past_time(self, past_time: timedelta):
        self._past_time = past_time

    past_time = property(get_past_time, set_past_time)

    def _get_selection_logger_name(self) -> str:
        return f'{self.get_name()}:_selection'

    def get_selection_logger(self, name: Union[str, Auto] = AUTO) -> Logger:
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

    def get_logger(self, skip_missing: bool = False) -> ExtendedLoggerInterface:
        logger = self.get_parent()
        assert isinstance(logger, ExtendedLoggerInterface) or hasattr(logger, 'log')
        return logger

    logger = property(get_logger)

    def log(
            self,
            msg: str,
            level: Union[LoggingLevel, Auto] = AUTO,
            end: Union[str, Auto] = AUTO,
            verbose: Union[bool, Auto] = AUTO,
    ) -> None:
        logger = self.get_logger()
        if logger is not None:
            logger.log(
                logger=self.get_logger(),
                msg=msg, level=level, end=end,
                verbose=Auto.acquire(verbose, self.is_verbose()),
            )

    def log_selection_batch(self, level: Union[LoggingLevel, Auto] = AUTO, reset_after: bool = True) -> None:
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

    def get_percent(self, round_digits: int = 1, default_value: str = 'UNK') -> str:
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
        start_time_str = f'{self.start_time.hour:02}:{self.start_time.minute:02}'
        past_total_minutes = int(self.past_time.total_seconds() / SECONDS_IN_MINUTE)
        past_remainder_minutes = past_total_minutes % MINUTES_IN_HOUR
        past_round_minutes = int(round(self.past_time.total_seconds() / SECONDS_IN_MINUTE, 0))
        past_hours = int(past_total_minutes / MINUTES_IN_HOUR)
        past_seconds = int(self.past_time.total_seconds()) % SECONDS_IN_MINUTE
        if self.is_finished():
            finish_time = datetime.now()
            self.past_time = finish_time - self.start_time
            past_time_str = f"{past_hours:02}:{past_remainder_minutes:02}'{past_seconds:02}"
            finish_time_str = f'{finish_time.hour:02}:{finish_time.minute:02}'
            return f'{start_time_str}+{past_time_str}={finish_time_str}'
        elif self.expected_count:
            finish_time = self.start_time + self.past_time / (self.evaluate_share() or SMALL_SHARE)
            rest_time = finish_time - datetime.now()
            past_time_str = f'{past_round_minutes:02}'
            finish_time_str = f'{finish_time.hour:02}:{finish_time.minute:02}'
            rest_minutes = int(rest_time.total_seconds() / SECONDS_IN_MINUTE)
            rest_seconds = int(rest_time.total_seconds()) % SECONDS_IN_MINUTE
            has_long_rest_time = rest_time.total_seconds() >= 100 * SECONDS_IN_MINUTE
            if has_long_rest_time:
                rest_time_str = f'{rest_minutes:03}'
                return f'{start_time_str}+{past_time_str}+{rest_time_str}~{finish_time_str}'
            else:
                rest_time_str = f"{rest_minutes:02}'{rest_seconds:02}"
                return f'{start_time_str}+{past_time_str}+{rest_time_str}~{finish_time_str}'
        else:
            past_time_str = f"{past_total_minutes:02}'{past_seconds:02}"
            return f'{start_time_str}+{past_time_str}'

    def update_now(self, cur: Optional[int]):
        if cur is not None:
            self.set_position(cur)
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
        if self.has_timing():
            line = '{} {} ({} it/sec)'.format(self.get_timing_str(), line, self.evaluate_speed())
        self.log(line, level=LoggingLevel.Debug, end='\r')

    def update_with_step(self, position: int, step: Union[int, Auto] = AUTO):
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

    def update(self, position: int, step: Union[int, Auto, None] = None, message: Optional[str] = None):
        if Auto.is_defined(message):
            self.set_name(message, inplace=True)
        if step == 1 or not Auto.is_defined(step):
            self.update_now(position)
        else:
            self.update_with_step(position, step)

    def start(self, position: int = 0) -> None:
        self.state = OperationStatus.InProgress
        self.start_time = datetime.now()
        self.position = position or self.position or 0
        if self.position != position:
            self.update(position)
        elif self.is_verbose():
            name = self.get_name()
            count = self.get_expected_count()
            self.log(f'{name} ({count} items): starting...')

    def finish(self, position: Optional[int] = None, log_selection_batch: bool = True) -> None:
        self.expected_count = position
        self.update(position)
        message = f'{self.get_name()}: Done. {position + 1} items processed'
        if self.has_timing():
            timing = self.get_timing_str()
            speed = self.evaluate_speed()
            message = f'{timing} {message} ({speed} it/sec)'
        self.log(message)
        if log_selection_batch:
            self.log_selection_batch()

    def iterate(
            self,
            items: Iterable,
            name: Optional[str] = None,
            expected_count: Optional[int] = None,
            step: Union[int, Auto] = AUTO,
            log_selection_batch: bool = True,
    ) -> Generator:
        if Auto.is_defined(name):
            self.set_name(name, inplace=True)
        if isinstance(items, (set, list, tuple)):
            self.expected_count = len(items)
        elif expected_count:
            self.expected_count = expected_count
        n = 0
        step = Auto.acquire(step, DEFAULT_STEP)
        self.start()
        for n, item in enumerate(items):
            self.update(n, step)
            yield item
        self.finish(n, log_selection_batch=log_selection_batch)
