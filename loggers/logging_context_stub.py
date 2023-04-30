from typing import Optional, Union

try:  # Assume we're a submodule in a package.
    from utils.decorators import singleton
    from interfaces import ContextInterface, LoggerInterface, ExtendedLoggerInterface, Name
    from base.abstract.tree_item import TreeItem
    from loggers.extended_logger import SingletonLogger
    from loggers.message_collector import SelectionMessageCollector
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils.decorators import singleton
    from ..interfaces import ContextInterface, LoggerInterface, ExtendedLoggerInterface, Name
    from ..base.abstract.tree_item import TreeItem
    from .extended_logger import SingletonLogger
    from .message_collector import SelectionMessageCollector

Native = ContextInterface
Child = Native

NAME = 'logging_context_stub'


@singleton
class LoggingContextStub(TreeItem, ContextInterface):
    def __init__(
            self,
            name: Optional[Name] = None,
            logger: Optional[LoggerInterface] = None,
            skip_not_implemented: bool = True
    ):
        if name is None:
            name = NAME
        self._logger = logger
        self._local_storage = None
        self._skip_not_implemented = skip_not_implemented
        self._tmp_folder = None
        super().__init__(name=name)

    def set_logger(self, logger: LoggerInterface, inplace: bool = False) -> Optional[Native]:
        self._logger = logger
        if hasattr(logger, 'get_context'):
            if not logger.get_context():
                if hasattr(logger, 'set_context'):
                    logger.set_context(self)
        if not inplace:
            return self

    def get_logger(self, create_if_not_yet: bool = True) -> LoggerInterface:
        logger = self._logger
        if isinstance(logger, LoggerInterface) or hasattr(logger, 'log'):
            if isinstance(logger, ExtendedLoggerInterface) or hasattr(logger, 'get_context'):
                if not logger.get_context():
                    if hasattr(logger, 'set_context'):
                        logger.set_context(self)
            return logger
        elif create_if_not_yet:
            return self.get_new_logger()

    def get_new_logger(self) -> LoggerInterface:
        return SingletonLogger(name=NAME, context=self)

    @staticmethod
    def get_new_selection_logger(name: Optional[Name], **kwargs) -> LoggerInterface:
        return SelectionMessageCollector(name, **kwargs)

    def get_selection_logger(self, name: Optional[Name] = None, **kwargs) -> LoggerInterface:
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

    def log(self, msg: str, level=None, end: Optional[str] = None, truncate: bool = True, verbose: bool = True) -> None:
        logger = self.get_logger()
        if isinstance(logger, ExtendedLoggerInterface):
            logger.log(msg=msg, level=level, end=end, truncate=truncate, verbose=verbose)
        elif logger is not None:
            logger.log(msg=msg, level=level)

    def add_child(self, name_or_child: Union[Name, Child], check: bool = True, inplace: bool = True) -> Optional[Child]:
        name, child = self._get_name_and_child(name_or_child)
        if self.is_logger(child):
            if hasattr(child, 'is_common_logger'):
                if child.is_common_logger():
                    self.set_logger(child)
        if not child.get_context():
            child.set_context(self)
        if not inplace:
            return self

    @staticmethod
    def is_logger(obj) -> bool:
        return isinstance(obj, LoggerInterface)

    @staticmethod
    def is_context() -> bool:
        return True

    def set_context(self, context: ContextInterface, reset: bool = True, inplace: bool = True):
        if not inplace:
            return self

    def _method_stub(self, method_name='called') -> None:
        msg_template = '{} method not implemented for {} class (use SnakeeContext class instead)'
        message = msg_template.format(method_name, self.__class__.__name__)
        if self._skip_not_implemented:
            logger = self.get_logger(create_if_not_yet=False)
            if logger:
                logger.warning(message, stacklevel=3)
        else:
            raise NotImplementedError(message)

    def get_local_storage(self, name='filesystem', create_if_not_yet=True):
        self._method_stub('get_local_storage')

    def get_job_folder(self, path='tmp'):
        self._method_stub('get_job_folder')

    def get_tmp_folder(self, path='tmp'):
        self._method_stub('get_tmp_folder')

    def get_stream(self, name, skip_missing=True):
        self._method_stub('get_stream')

    def get_connection(self, name, skip_missing=True):
        self._method_stub('get_connection')

    def conn(self, conn, name=None, check=True, redefine=True, **kwargs):
        self._method_stub('conn')

    def stream(self, item_type, name=None, check=True, **kwargs):
        self._method_stub('stream')

    def rename_stream(self, old_name, new_name):
        self._method_stub('rename_stream')

    def clear_tmp_files(self, verbose=True):
        self._method_stub('clear_tmp_files')

    def close_conn(self, name, recursively=False, verbose=True):
        self._method_stub('close_conn')

    def close_stream(self, name, recursively=False, verbose=True):
        self._method_stub('close_stream')

    def forget_conn(self, name, recursively=True, skip_errors=False, verbose=True):
        self._method_stub('forget_conn')

    def forget_stream(self, name, recursively=True, skip_errors=False, verbose=True):
        self._method_stub('forget_stream')

    def close_all_conns(self, recursively=False, verbose=True):
        self._method_stub('close_all_conns')

    def close_all_streams(self, recursively=False, verbose=True):
        self._method_stub('close_all_streams')

    def close(self, verbose=True):
        self._method_stub('close')

    def forget_all_conns(self, recursively=False, verbose=True):
        self._method_stub('forget_all_conns')

    def forget_all_streams(self, recursively=False, verbose=True):
        self._method_stub('forget_all_streams')

    def forget_all_children(self):
        self._method_stub('forget_all_children')

    def get_parent(self):
        self._method_stub('get_parent')

    def set_parent(self, parent, reset=False, inplace=True):
        self._method_stub('set_parent')

    def get_items(self):
        self._method_stub('get_items')

    def get_children(self):
        self._method_stub('get_children')

    def get_child(self, name):
        self._method_stub('get_child')

    def forget_child(self, name_or_child, recursively=False, also_from_context=True, skip_errors=False):
        self._method_stub('forget_child')

    def is_leaf(self):
        self._method_stub('is_leaf')

    def is_root(self):
        self._method_stub('is_root')
