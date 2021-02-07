try:  # Assume we're a sub-module in a package.
    from connectors import connector_classes as ct
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import connector_classes as ct


PATH_DELIMITER = '/'


class LocalStorage(ct.AbstractStorage):
    def __init__(
            self,
            name='filesystem',
            context=None,
            verbose=True,
            path_delimiter=PATH_DELIMITER,
    ):
        super().__init__(
            name=name,
            context=context,
            verbose=verbose,
        )
        self.path_delimiter = path_delimiter

    @staticmethod
    def get_default_child_class():
        return ct.LocalFolder

    def get_folders(self):
        return self.children

    def folder(self, name, **kwargs):
        return self.child(name, **kwargs)

    def get_path_delimiter(self):
        return self.path_delimiter
