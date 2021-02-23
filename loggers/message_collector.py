try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from loggers import logger_classes as log
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from . import logger_classes as log


DEFAULT_MESSAGE_COLLECTOR_NAME = 'MessageCollector'
SELECTION_MESSAGE_COLLECTOR_NAME = 'SelectorMessageCollector'


class MessageCollector(log.ExtendedLogger):
    def __init__(
            self,
            name=DEFAULT_MESSAGE_COLLECTOR_NAME,
            level=arg.DEFAULT, formatter=arg.DEFAULT, max_line_len=arg.DEFAULT,
            context=None,
            max_keys=None, max_items=None,
    ):
        super().__init__(name=name, level=level, formatter=formatter, max_line_len=max_line_len, context=context)
        self.counts = dict()
        self.examples = dict()
        self.max_keys = max_keys
        self.max_items = max_items

    def get_name(self):
        return self.name

    def set_name(self, name):
        self.name = name

    def get_keys(self):
        return self.counts.keys()

    def get_key_count(self):
        return len(self.counts)

    def get_count(self, key):
        return self.counts.get(key)

    def get_counts(self):
        return self.counts

    def get_examples(self, key):
        return self.examples.get(key, [])

    def get_example_count(self, key):
        return len(self.get_examples(key))

    def add_message(self, message: log.DetailedMessage, key_as_str=True):
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

    def log(self, msg, level=arg.DEFAULT, logger=None, end=arg.DEFAULT, verbose=False):
        level = arg.undefault(level, log.LoggingLevel.Debug)
        if isinstance(msg, str):
            msg = log.DetailedMessage(message=msg)
        self.add_message(msg)
        if logger or verbose:
            super().log(msg, level, logger, end, verbose)

    def get_str(self):
        return '{}: {}'.format(self.name, ', '.join(['{}: {}'.format(k, v) for k, v in self.get_counts().items()]))


class SelectionMessageCollector(MessageCollector):
    def __init__(
            self,
            name=SELECTION_MESSAGE_COLLECTOR_NAME,
            level=arg.DEFAULT, formatter=arg.DEFAULT, max_line_len=arg.DEFAULT,
            context=None,
            max_keys=10, max_items=5, ok_key='ok',
    ):
        super().__init__(
            name=name,
            level=level, formatter=formatter, max_line_len=max_line_len, context=context,
            max_keys=max_keys, max_items=max_items,
        )
        self.ok_key = ok_key
        self.counts[ok_key] = 0

    def set_options(self, **kwargs):
        self.__dict__.update(kwargs)

    def get_ok_key(self):
        return self.ok_key

    def get_err_keys(self):
        return [i for i in self.get_unordered_keys() if i != self.get_ok_key()]

    def get_unordered_keys(self):
        return super().get_keys()

    def get_ordered_keys(self):
        return [self.get_ok_key()] + self.get_err_keys()

    def get_keys(self, ordered=True):
        if ordered:
            return self.get_ordered_keys()
        else:
            return self.get_unordered_keys()

    def is_new_key(self, message: log.SelectionError, key_as_str=True):
        if key_as_str:
            key = message.get_key_str()
        else:
            key = message.get_key_values()
        if key not in self.get_keys():
            return True

    def get_message_batch(self, as_str=True):
        for key in self.get_ordered_keys():
            if as_str:
                yield 'Selection {} ({})'.format(key, self.get_count(key))
            else:
                yield key, self.get_count(key)

    def log_selection_error(self, func, in_fields, in_values, in_record, message):
        msg = log.SelectionError(func, in_fields, in_values, in_record, message)
        is_new_key = self.is_new_key(msg)
        self.add_message(msg)
        if is_new_key:
            self.show_error(msg)

    def show_error(self, message: log.SelectionError):
        msg = '{} selection errors with {} keys: {}'.format(
            self.get_err_count(), self.get_key_count(), message.get_str(),
        )
        context = self.get_context()
        if context:
            context.get_logger().show(msg, end='\r')
        else:
            print(msg, end='\r')

    def has_errors(self):
        return bool(self.get_err_keys())

    def get_err_count(self):
        return sum([self.get_count(k) for k in self.get_err_keys()])


class CommonMessageCollector(SelectionMessageCollector):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(CommonMessageCollector, cls).__new__(cls)
        return cls.instance
