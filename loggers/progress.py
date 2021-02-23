from datetime import timedelta, datetime

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from loggers import logger_classes as log
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from . import logger_classes as log


class Progress:
    def __init__(
            self,
            name='Progress',
            count=None,
            timing=True,
            verbose=True,
            logger=arg.DEFAULT,
            context=None,
    ):
        self.name = name
        self.expected_count = count
        self.verbose = verbose
        self.state = log.OperationStatus.New
        self.position = 0
        self.timing = timing
        self.start_time = None
        self.past_time = timedelta(0)
        if logger is None:
            self.logger = None
        elif logger == arg.DEFAULT:
            self.logger = context.get_logger() if context else log.get_logger()
        else:
            self.logger = logger
        if context:
            self.context = context
        elif logger:
            self.context = logger.get_context()
        else:
            self.context = None
        self.selection_logger = None

    def get_context(self):
        return self.context

    def has_selection_logger(self):
        return self.selection_logger is not None

    def get_selection_logger(self):
        if not self.selection_logger:
            if self.get_context():
                self.selection_logger = self.get_context().get_selection_logger()
                self.selection_logger.set_name(self.name)
            else:
                self.selection_logger = log.SelectionMessageCollector(self.name)
        return self.selection_logger

    def reset_selection_logger(self, **kwargs):
        self.get_logger().reset_selection_logger(**kwargs)

    def get_logger(self):
        return self.logger

    def log(self, msg, level=arg.DEFAULT, end=arg.DEFAULT, verbose=arg.DEFAULT):
        logger = self.get_logger()
        if logger is not None:
            logger.log(
                logger=self.get_logger(),
                msg=msg, level=level, end=end,
                verbose=arg.undefault(verbose, self.verbose),
            )

    def log_selection_batch(self, level=arg.DEFAULT, reset_after=True):
        selection = self.get_selection_logger()
        if selection.has_errors():
            batch = selection.get_message_batch()
            for msg in batch:
                self.log(msg, level=level)
        if reset_after:
            self.reset_selection_logger()

    def is_started(self):
        return self.start_time is not None

    def is_finished(self):
        if self.expected_count:
            return self.position >= self.expected_count

    def get_percent(self, round_digits=1, default_value='UNK'):
        share = self.evaluate_share()
        if share is None or share > 1:
            return default_value
        else:
            share = round(100 * share, round_digits)
            return '{}%'.format(share)

    def evaluate_share(self):
        if self.expected_count:
            return (self.position + 1) / self.expected_count

    def evaluate_speed(self):
        cur_time = datetime.now()
        self.past_time = cur_time - self.start_time
        if self.past_time.total_seconds():
            speed = self.position / self.past_time.total_seconds()
            return int(speed)

    def get_timing_str(self):
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
            return "{:02}:{:02}+{:02}".format(
                self.start_time.hour, self.start_time.minute, int(round(self.past_time.total_seconds() % 60, 0)),
            )

    def update_now(self, cur):
        self.position = cur or self.position or 0
        if self.state != log.OperationStatus.InProgress:
            self.start(cur)
        if self.expected_count:
            line = '{name}: {percent} ({pos}/{count}) processed'.format(
                name=self.name,
                percent=self.get_percent(),
                pos=self.position + 1,
                count=self.expected_count,
            )
        else:
            line = '{}: {} items processed'.format(self.name, self.position + 1)
        if self.has_selection_logger():
            line = '{}, {} errors'.format(line, self.get_selection_logger().get_err_count())
        if self.timing:
            line = '{} {} ({} it/sec)'.format(self.get_timing_str(), line, self.evaluate_speed())
        self.log(line, level=log.LoggingLevel.Debug, end='\r')

    def update_with_step(self, position, step=arg.DEFAULT):
        step = arg.undefault(step, log.DEFAULT_STEP)
        cur_increment = position - (self.position or 0)
        self.position = position
        step_passed = (self.position + 1) % step == 0
        step_passed = step_passed or (cur_increment >= step)
        pool_finished = 0 < (self.expected_count or 0) < (self.position + 1)
        if step_passed or pool_finished:
            self.update_now(position)

    def update(self, position, step=None, message=None):
        if message and message != arg.DEFAULT:
            self.name = message
        if step is None or step == 1:
            self.update_now(position)
        else:
            self.update_with_step(position, step)

    def start(self, position=0):
        self.state = log.OperationStatus.InProgress
        self.start_time = datetime.now()
        self.position = position or self.position or 0
        if self.position != position:
            self.update(position)
        elif self.verbose:
            self.log('{} ({} items): starting...'.format(self.name, self.expected_count))

    def finish(self, position=None, log_selection_batch=True):
        self.expected_count = position
        self.update(position)
        message = '{}: Done. {} items processed'.format(self.name, self.position + 1)
        if self.timing:
            message = '{} {} ({} it/sec)'.format(self.get_timing_str(), message, self.evaluate_speed())
        self.log(message)
        if log_selection_batch:
            self.log_selection_batch()

    def iterate(self, items, name=None, expected_count=None, step=arg.DEFAULT, log_selection_batch=True):
        self.name = name or self.name
        if isinstance(items, (set, list, tuple)):
            self.expected_count = len(items)
        else:
            self.expected_count = expected_count or self.expected_count
        n = 0
        self.start()
        for n, item in enumerate(items):
            self.update(n, step)
            yield item
        self.finish(n, log_selection_batch=log_selection_batch)
