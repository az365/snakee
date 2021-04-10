from typing import Iterable

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from connectors.abstract.abstract_storage import AbstractStorage
    from connectors.filesystem.local_folder import LocalFolder
    from loggers.extended_logger import SingletonLogger
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ..abstract.abstract_storage import AbstractStorage
    from ..filesystem.local_folder import LocalFolder
    from ...loggers.extended_logger import SingletonLogger

PATH_DELIMITER = '/'


class LocalStorage(AbstractStorage):
    def __init__(
            self,
            name='filesystem',
            context=arg.DEFAULT,
            verbose=True,
            path_delimiter=PATH_DELIMITER,
    ):
        if arg.is_defined(context):
            registered_local_storage = context.get_local_storage(create_if_not_yet=False)
            if registered_local_storage:
                assert name != registered_local_storage.get_name(), 'Default local storage already registered'
        self.path_delimiter = path_delimiter
        super().__init__(name=name, context=context, verbose=verbose)

    def get_logger(self, skip_missing=False, create_if_not_yet=True):
        context = self.get_context()
        if context:
            return context.get_logger(create_if_not_yet=create_if_not_yet)
        elif create_if_not_yet:
            return SingletonLogger()

    @staticmethod
    def get_default_child_class():
        return LocalFolder

    def get_folders(self) -> Iterable:
        for name, folder in self.get_children():
            yield folder

    def folder(self, name, **kwargs):
        return self.child(name, parent=self, **kwargs)

    def get_path_delimiter(self):
        return self.path_delimiter
