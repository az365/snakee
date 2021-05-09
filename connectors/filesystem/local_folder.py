import os
import fnmatch
from typing import Union, Iterable, Optional

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from utils.enum import ClassType
    from base.interfaces.context_interface import ContextInterface
    from connectors.abstract.connector_interface import ConnectorInterface
    from connectors.abstract.hierarchic_connector import HierarchicConnector
    from connectors.abstract.abstract_folder import HierarchicFolder
    from connectors.filesystem.local_file import TextFile
    from connectors.filesystem.file_type import FileType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...utils.enum import ClassType
    from ...base.interfaces.context_interface import ContextInterface
    from ..abstract.connector_interface import ConnectorInterface
    from ..abstract.hierarchic_connector import HierarchicConnector
    from ..abstract.abstract_folder import HierarchicFolder
    from .local_file import TextFile
    from .file_type import FileType

PARENT_TYPES = HierarchicConnector, ConnectorInterface, ContextInterface


class FolderType(ClassType):
    LocalStorage = 'LocalStorage'
    LocalFolder = 'LocalFolder'
    FileMask = 'FileMask'

    @staticmethod
    def detect_by_name(name: str):
        if '*' in name:
            return FolderType.FileMask
        else:
            return FolderType.LocalFolder

    @classmethod
    def prepare(cls):
        super().prepare()
        cls.set_dict_classes({FolderType.LocalFolder: LocalFolder, FolderType.FileMask: FileMask})


class LocalFolder(HierarchicFolder):
    def __init__(
            self,
            path: str,
            path_is_relative: Union[bool, arg.DefaultArgument] = arg.DEFAULT,
            parent: Union[HierarchicConnector, ConnectorInterface, arg.DefaultArgument] = arg.DEFAULT,
            context: Optional[ContextInterface] = None,
            verbose: Union[bool, arg.DefaultArgument] = arg.DEFAULT,
    ):
        if not arg.is_defined(parent):
            parent = self.get_default_parent()
        if parent:
            assert isinstance(parent, PARENT_TYPES), 'got {} as {}'.format(parent, type(parent))
        elif context:
            parent = context.get_local_storage()
        super().__init__(
            name=path,
            parent=parent,
            verbose=verbose,
        )
        path_is_relative = arg.undefault(path_is_relative, not arg.is_absolute_path(path))
        self._path_is_relative = path_is_relative

    @staticmethod
    def get_default_parent(context=arg.DEFAULT):
        if arg.is_defined(context):
            return context.get_local_storage()

    def get_default_child_class(self):
        return TextFile

    @staticmethod
    def get_child_class_by_type(type_name: Union[FolderType, FileType, str]):
        try:
            conn_type = FolderType(type_name)
        except ValueError:
            conn_type = FileType(type_name)
        return conn_type.get_class()

    @staticmethod
    def get_type_by_name(name) -> Union[FileType, FolderType]:
        if '*' in name:
            return FolderType.FileMask
        else:
            return FileType.detect_by_name(name)

    def get_child_class_by_name(self, name):
        supposed_type = self.get_type_by_name(name)
        return self.get_child_class_by_type(supposed_type)

    def get_child_class_by_name_and_type(self, name: str, filetype: Union[FileType, arg.DefaultArgument] = arg.DEFAULT):
        if arg.is_defined(filetype):
            return FileType(filetype).get_class()
        else:
            supposed_type = self.get_type_by_name(name)
            if supposed_type:
                return supposed_type.get_class()

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
            assert file_class, "filetype isn't detected"
            file = file_class(filename, folder=self, **kwargs)
            self.get_children()[name] = file
        return file

    def folder(self, name, folder_type=arg.DEFAULT, **kwargs):
        supposed_type = FolderType.detect_by_name(name)
        folder_type = arg.undefault(folder_type, supposed_type)
        folder_class = FolderType(folder_type).get_class()
        folder_obj = folder_class(name, parent=self, **kwargs)
        self.add_folder(folder_obj)
        return folder_obj

    def mask(self, mask: str):
        return self.folder(mask, FolderType.FileMask)

    def add_file(self, file):
        assert file.is_leaf(), 'file must be an instance of *File (got {})'.format(type(file))
        super().add_child(file)

    def add_folder(self, folder):
        assert folder.is_folder()
        super().add_child(folder)

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
            for file in self.get_files():
                closed_count += file.close() or 0
        return closed_count

    def has_parent_folder(self) -> bool:
        parent = self.get_parent()
        if hasattr(parent, 'is_folder'):
            return parent.is_folder()

    def is_relative_path(self) -> bool:  # just path this object
        return self._path_is_relative

    def has_relative_path(self) -> bool:  # path including parent folder
        if self.has_parent_folder():
            return self.get_parent().has_path_relative()
        else:
            return self.is_relative_path()

    def get_path(self) -> str:
        if self.is_relative_path() and not self.has_parent_folder():
            return self.get_name()
        else:
            return super().get_path()

    def get_full_path(self) -> str:
        if self.has_relative_path():
            current_path = self.get_storage().get_full_path()
            name = self.get_name()
            if name:
                current_path += self.get_path_delimiter() + name
            return current_path
        else:
            return self.get_path()

    def get_folder_path(self) -> str:
        return self.get_path()

    def get_file_path(self, name) -> str:
        if self.get_folder_path():
            return self.get_folder_path() + self.get_path_delimiter() + name
        else:
            return name

    def is_existing(self) -> bool:
        return os.path.exists(self.get_path())

    def list_existing_names(self) -> list:
        if self.get_name() in ['', '.']:
            return os.listdir()
        else:
            return os.listdir(self.get_path())

    def get_existing_file_names(self) -> Iterable:
        for name in self.list_existing_names():
            path = self.get_file_path(name)
            if os.path.isfile(path):
                yield name

    def list_existing_file_names(self) -> Iterable:
        return list(self.get_existing_file_names())

    def all_existing_files(self, **kwargs):
        for name in self.list_existing_file_names():
            yield self.file(name, **kwargs)


class FileMask(LocalFolder):
    def __init__(
            self,
            mask: str,
            parent: HierarchicConnector,
            context=None,
            verbose: Union[bool, arg.DefaultArgument] = arg.DEFAULT,
    ):
        if not arg.is_defined(parent):
            if arg.is_defined(context):
                parent = context.get_local_storage()
        assert parent.is_folder() or parent.is_storage()
        super().__init__(path=mask, parent=parent, context=context, verbose=verbose)

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


FolderType.prepare()
