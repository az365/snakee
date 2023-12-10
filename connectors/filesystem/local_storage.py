from typing import Type, Callable, Iterable, Union
import os

try:  # Assume we're a submodule in a package.
    from interfaces import ConnType, ConnectorInterface, Context
    from base.constants.chars import SLASH
    from connectors.abstract.abstract_storage import AbstractStorage
    from loggers.extended_logger import SingletonLogger
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import ConnType, ConnectorInterface, Context
    from ...base.constants.chars import SLASH
    from ..abstract.abstract_storage import AbstractStorage
    from ...loggers.extended_logger import SingletonLogger

Class = Union[Type, Callable]

PATH_DELIMITER = SLASH


class LocalStorage(AbstractStorage):
    def __init__(
            self,
            name: str = 'filesystem',
            context: Context = None,
            verbose: bool = True,
            path_delimiter: str = PATH_DELIMITER,
    ):
        if context:
            registered_local_storage = context.get_local_storage(create_if_not_yet=False)
            if registered_local_storage:
                assert name != registered_local_storage.get_name(), 'Default local storage already registered'
        self._path_delimiter = path_delimiter
        super().__init__(name=name, context=context, verbose=verbose)

    def get_logger(self, skip_missing=False, create_if_not_yet=True):
        context = self.get_context()
        if context:
            return context.get_logger(create_if_not_yet=create_if_not_yet)
        elif create_if_not_yet:
            return SingletonLogger()

    @staticmethod
    def get_default_child_type() -> ConnType:
        return ConnType.LocalFolder

    @classmethod
    def get_default_child_class(cls) -> Class:
        child_class = cls.get_default_child_type().get_class
        if child_class is None:
            child_class = cls.get_default_child_obj_class()
        return child_class

    def get_folders(self) -> Iterable:
        for name, folder in self.get_children():
            yield folder

    def folder(self, name, **kwargs) -> ConnectorInterface:
        return self.child(name, parent=self, **kwargs)

    def get_path_delimiter(self) -> str:
        return self._path_delimiter

    @staticmethod
    def get_full_path() -> str:
        return os.getcwd()


ConnType.add_classes(LocalStorage)
