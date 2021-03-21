from typing import Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.interfaces.context_interface import ContextInterface
    from base.interfaces.contextual_interface import ContextualInterface
    from base.abstract.tree_item import TreeItem
    from loggers.logger_interface import LoggerInterface
    from loggers.extended_logger import SingletonLogger
    from loggers.message_collector import SelectionMessageCollector
    from connectors.filesystem.local_storage import LocalStorage
    from connectors.filesystem.local_folder import LocalFolder
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from ..base.interfaces.context_interface import ContextInterface
    from ..base.interfaces.contextual_interface import ContextualInterface
    from ..base.abstract.tree_item import TreeItem
    from .logger_interface import LoggerInterface
    from .extended_logger import SingletonLogger
    from .message_collector import SelectionMessageCollector
    from ..connectors.filesystem.local_storage import LocalStorage
    from ..connectors.filesystem.local_folder import LocalFolder

NAME = 'logging_context_stub'


class LoggingContextStub(TreeItem, ContextInterface):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(LoggingContextStub, cls).__new__(cls)
        return cls.instance

    def __init__(
            self,
            name=arg.DEFAULT,
            logger=arg.DEFAULT,
            skip_not_implemented: bool = True
    ):
        super().__init__(name=arg.undefault(name, NAME))
        self._logger = logger
        self._local_storage = None
        self._skip_not_implemented = skip_not_implemented

    def set_logger(self, logger: LoggerInterface):
        self._logger = logger
        if hasattr(logger, 'get_context'):
            if not logger.get_context():
                if hasattr(logger, 'set_context'):
                    logger.set_context(self)

    def get_logger(self, create_if_not_yet=True) -> LoggerInterface:
        if arg.is_defined(self._logger):
            if not self._logger.get_context():
                self._logger.set_context(self)
            return self._logger
        elif create_if_not_yet:
            return self.get_new_logger()

    def get_new_logger(self):
        return SingletonLogger(name=NAME, context=self)

    @staticmethod
    def get_new_selection_logger(name, **kwargs):
        return SelectionMessageCollector(name, **kwargs)

    def get_selection_logger(self, name=arg.DEFAULT, **kwargs):
        logger = self.get_logger()
        if hasattr(logger, 'get_selection_logger'):
            selection_logger = logger.get_selection_logger(name, **kwargs)
        else:
            selection_logger = None
        if not selection_logger:
            selection_logger = self.get_new_selection_logger(name, **kwargs)
            if hasattr(logger, 'set_selection_logger'):
                logger.set_selection_logger(selection_logger)
        return selection_logger

    def log(self, msg, level=arg.DEFAULT, end=arg.DEFAULT, verbose=True):
        logger = self.get_logger()
        if logger is not None:
            logger.log(
                msg=msg, level=level,
                end=end, verbose=verbose,
            )

    def add_child(self, instance: ContextualInterface):
        if self.is_logger(instance):
            if hasattr(instance, 'is_common_logger'):
                if instance.is_common_logger():
                    self.set_logger(instance)
        elif isinstance(instance, LocalStorage):
            self._local_storage = instance
        if not instance.get_context():
            instance.set_context(self)

    def get_local_storage(self, name='filesystem', create_if_not_yet=True):
        if self._local_storage:
            if self._local_storage.get_name() == name:
                return self._local_storage
        if create_if_not_yet:
            self._local_storage = LocalStorage(name, context=self)
        return self._local_storage

    def get_job_folder(self, path='tmp'):
        return LocalFolder(path, parent=self.get_local_storage())

    def get_tmp_folder(self, path='tmp'):
        tmp_files_template = '{}/{}.tmp'.format(path, '{}')
        return LocalFolder(tmp_files_template, parent=self.get_local_storage())

    @staticmethod
    def is_logger(obj):
        return isinstance(obj, LoggerInterface)

    @staticmethod
    def is_context() -> bool:
        return True

    def method_stub(self, method_name='called', *args, **kwargs):
        msg_template = '{} method not implemented for {} class (use SnakeeContext class instead)'
        message = msg_template.format(method_name, self.__class__.__name__)
        if self._skip_not_implemented:
            logger = self.get_logger(create_if_not_yet=False)
            if logger:
                logger.warning(message)
        else:
            raise NotImplemented(message)

    def get_stream(self, name: str):
        self.method_stub()

    def get_connection(self, name: str):
        self.method_stub()

    def conn(self, conn, name: str, check: bool, redefine: bool, **kwargs):
        self.method_stub()

    def stream(self, stream_type, name: str, check: bool, **kwargs):
        self.method_stub()

    def rename_stream(self, old_name: str, new_name: str):
        self.method_stub()

    def close_conn(self, name: str, recursively: bool, verbose: bool):
        self.method_stub()

    def close_stream(self, name, recursively=False, verbose=True):
        self.method_stub()

    def forget_conn(self, name, recursively=True, verbose=True):
        self.method_stub()

    def forget_stream(self, name, recursively=True, verbose=True):
        self.method_stub()

    def close_all_conns(self, recursively=False, verbose=True):
        self.method_stub()

    def close_all_streams(self, recursively=False, verbose=True):
        self.method_stub()

    def close(self, verbose=True):
        self.method_stub()

    def forget_all_conns(self, recursively=False):
        self.method_stub()

    def forget_all_streams(self, recursively=False):
        self.method_stub()

    def forget_all_children(self):
        self.method_stub()

    def get_name(self):
        self.method_stub()

    def get_parent(self):
        self.method_stub()

    def set_parent(self, parent, reset: bool = False, inplace: bool = True):
        self.method_stub()

    def get_items(self):
        self.method_stub()

    def get_children(self):
        self.method_stub()

    def get_child(self, name: str):
        self.method_stub()

    def forget_child(self, child_or_name: Union[ContextualInterface, str], skip_errors=False):
        self.method_stub()

    def is_leaf(self):
        self.method_stub()

    def is_root(self):
        self.method_stub()

    def set_context(self, context: ContextInterface, reset=True, inplace=True):
        pass
