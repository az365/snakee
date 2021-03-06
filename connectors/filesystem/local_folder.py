from enum import Enum
import os
import fnmatch

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


class LocalFolder(ct.HierarchicFolder):
    def __init__(
            self,
            path,
            path_is_relative=True,
            parent=arg.DEFAULT,
            context=arg.DEFAULT,
            verbose=arg.DEFAULT,
    ):
        parent = arg.undefault(parent, ct.LocalStorage(context=context))
        assert isinstance(parent, (LocalFolder, ct.LocalFolder, ct.LocalStorage))
        super().__init__(
            name=path,
            parent=parent,
            verbose=verbose,
        )
        self.path_is_relative = path_is_relative

    def get_default_child_class(self):
        return ct.TextFile

    @staticmethod
    def get_child_class_by_type(filetype):
        return ct.ConnType(filetype).get_class()

    @staticmethod
    def get_type_by_name(name):
        if '*' in name:
            return ct.FileMask
        else:
            file_ext = name.split('.')[-1]
            return ct.get_type_by_ext(file_ext)

    def get_child_class_by_name(self, name):
        supposed_type = self.get_type_by_name(name)
        return self.get_child_class_by_type(supposed_type)

    def get_child_class_by_name_and_type(self, name, filetype=arg.DEFAULT):
        supposed_type = self.get_type_by_name(name)
        filetype = arg.undefault(filetype, supposed_type)
        return self.get_child_class_by_type(filetype)

    def get_files(self):
        for item in self.get_items():
            if hasattr(item, 'is_leaf'):
                if item.is_leaf():
                    yield item

    def file(self, name, filetype=arg.DEFAULT, **kwargs):
        file = self.get_children().get(name)
        if kwargs or not file:
            filename = kwargs.pop('filename', name)
            file_class = self.get_child_class_by_name_and_type(name, filetype)
            file = file_class(filename, folder=self, **kwargs)
            self.get_children()[name] = file
        return file

    def folder(self, name, folder_type=arg.DEFAULT, **kwargs):
        supposed_type = ct.ConnType.FileMask if '*' in name else ct.ConnType.LocalFolder
        subfolder_type = arg.undefault(folder_type, supposed_type)
        subfolder_class = ct.ConnType(subfolder_type).get_class()
        subfolder_obj = subfolder_class(name, parent=self, **kwargs)
        self.add_folder(name, subfolder_obj)
        return subfolder_obj

    def mask(self, mask):
        return self.folder(mask, ct.ConnType.FileMask)

    def add_file(self, name, file):
        assert file.is_leaf(), 'file must be an instance of *File (got {})'.format(type(file))
        assert name not in self.get_children(), 'file with name {} is already registered'.format(name)
        self.get_children()[name] = file

    def add_folder(self, name, folder):
        assert folder.is_folder()
        assert name not in self.get_children()
        self.get_children()[name] = folder

    def get_links(self):
        for item in self.get_files():
            yield from item.get_links()

    def close(self, name=None):
        closed_count = 0
        if name:
            file = self.get_children().get(name)
            if file:
                closed_count += file.close() or 0
        else:
            for file in self.get_items():
                closed_count += file.close() or 0
        return closed_count

    def has_parent_folder(self):
        return self.get_parent().is_folder()

    def has_path_relative(self):
        if self.has_parent_folder():
            return self.get_parent().has_path_relative()
        else:
            return self.path_is_relative

    def get_path(self):
        if self.has_path_relative() and not self.has_parent_folder():
            return self.get_name()
        else:
            return super().get_path()

    def get_folder_path(self):
        return self.get_path()

    def get_file_path(self, name):
        if self.get_folder_path():
            return self.get_folder_path() + self.get_path_delimiter() + name
        else:
            return name

    def list_existing_names(self):
        if self.get_name() in ['', '.']:
            return os.listdir()
        else:
            return os.listdir(self.get_path())

    def yield_existing_file_names(self):
        for name in self.list_existing_names():
            path = self.get_file_path(name)
            if os.path.isfile(path):
                yield name

    def list_existing_file_names(self):
        return list(self.yield_existing_file_names())

    def all_existing_files(self, **kwargs):
        for name in self.list_existing_file_names():
            yield self.file(name, **kwargs)


class FileMask(LocalFolder):
    def __init__(
            self,
            mask,
            parent,
            context=None,
            verbose=arg.DEFAULT,
    ):
        parent = arg.undefault(parent, ct.LocalStorage(context=context))
        assert isinstance(parent, (LocalFolder, ct.LocalFolder, ct.LocalStorage))
        super().__init__(
            path=mask,
            parent=parent,
            verbose=verbose,
        )

    def get_mask(self):
        return self.get_name()

    def get_folder(self):
        return self.get_parent()

    def get_folder_path(self):
        return self.get_folder().get_path()

    def get_mask_path(self):
        return self.get_folder_path() + self.get_path_delimiter() + self.get_mask()

    def get_path(self, with_mask=True):
        if with_mask:
            return self.get_mask_path()
        else:
            return self.get_folder_path()

    def yield_existing_names(self):
        for name in self.get_folder().list_existing_names():
            if fnmatch.fnmatch(name, self.get_mask()):
                yield name

    def list_existing_names(self):
        return list(self.yield_existing_names())
