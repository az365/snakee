from enum import Enum

try:  # Assume we're a sub-module in a package.
    from connectors import connector_classes as ct
    from utils import arguments as arg
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import connector_classes as ct
    from ...utils import arguments as arg


class FileType(Enum):
    TextFile = 'TextFile'
    JsonFile = 'JsonFile'
    ColumnFile = 'ColumnFile'
    CsvFile = 'CsvFile'
    TsvFile = 'TsvFile'
    TskvFile = 'TskvFile'


class LocalFolder(ct.FlatFolder):
    def __init__(
            self,
            path,
            path_is_relative=True,
            storage=arg.DEFAULT,
            context=None,
            verbose=arg.DEFAULT,
    ):
        storage = arg.undefault(storage, ct.LocalStorage(context=context))
        assert isinstance(storage, ct.LocalStorage)
        super().__init__(
            name=path,
            parent=storage,
            verbose=verbose,
        )
        self.path_is_relative = path_is_relative

    def get_default_child_class(self):
        return ct.TextFile

    @staticmethod
    def get_child_class_by_filetype(filetype):
        return ct.get_class(filetype)

    @staticmethod
    def get_file_type_by_name(name):
        file_ext = name.split('.')[-1]
        return ct.DICT_EXT_TO_TYPE.get(file_ext, ct.ConnType.TextFile)

    def get_child_class_by_name(self, name):
        supposed_type = self.get_file_type_by_name(name)
        return self.get_child_class_by_filetype(supposed_type)

    def get_child_class_by_name_and_type(self, name, filetype=arg.DEFAULT):
        supposed_type = self.get_file_type_by_name(name)
        filetype = arg.undefault(filetype, supposed_type)
        return self.get_child_class_by_filetype(filetype)

    def get_files(self):
        return self.get_items()

    def file(self, name, filetype=arg.DEFAULT, **kwargs):
        file = self.get_files().get(name)
        if kwargs or not file:
            filename = kwargs.pop('filename', name)
            file_class = self.get_child_class_by_name_and_type(name, filetype)
            file = file_class(filename, folder=self, **kwargs)
            self.get_files()[name] = file
        return file

    def add_file(self, name, file):
        assert ct.is_file(file), 'file must be an instance of *File (got {})'.format(type(file))
        assert name not in self.get_files(), 'file with name {} is already registered'.format(name)
        self.get_files()[name] = file

    def get_links(self):
        for item in self.get_files():
            yield from item.get_links()

    def close(self, name=None):
        closed_count = 0
        if name:
            file = self.get_files().get(name)
            if file:
                closed_count += file.close() or 0
        else:
            for file in self.get_files().values():
                closed_count += file.close() or 0
        return closed_count

    def get_meta(self):
        meta = self.__dict__.copy()
        meta.pop('files')
        return meta

    def get_path(self):
        if self.path_is_relative:
            return self.get_name()
        else:
            return super().get_path()
