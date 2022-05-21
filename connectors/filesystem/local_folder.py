from typing import Optional, Iterable, Generator, Union
import os

try:  # Assume we're a submodule in a package.
    from interfaces import (
        ContextInterface, ConnectorInterface, Connector, ContentFormatInterface,
        ConnType, ContentType, Class, LoggingLevel,
        AUTO, Auto, AutoBool, AutoContext, AutoConnector,
    )
    from functions.primary.text import is_absolute_path
    from connectors.abstract.hierarchic_connector import HierarchicConnector
    from connectors.abstract.leaf_connector import LeafConnector
    from connectors.abstract.abstract_folder import HierarchicFolder
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        ContextInterface, ConnectorInterface, Connector, ContentFormatInterface,
        ConnType, ContentType, Class, LoggingLevel,
        AUTO, Auto, AutoBool, AutoContext, AutoConnector,
    )
    from ...functions.primary.text import is_absolute_path
    from ..abstract.hierarchic_connector import HierarchicConnector
    from ..abstract.leaf_connector import LeafConnector
    from ..abstract.abstract_folder import HierarchicFolder

Native = HierarchicFolder


class LocalFolder(HierarchicFolder):
    _default_storage: Connector = None

    def __init__(
            self,
            path: str,
            path_is_relative: AutoBool = AUTO,
            parent: AutoConnector = AUTO,
            context: AutoContext = None,
            verbose: AutoBool = AUTO,
    ):
        if not Auto.is_defined(parent):
            if Auto.is_defined(context):
                parent = context.get_local_storage()
            else:
                parent = self.get_default_storage()
        parent = self._assume_native(parent)
        self._path_is_relative = Auto.acquire(path_is_relative, not is_absolute_path(path))
        super().__init__(name=path, parent=parent, verbose=verbose)

    def is_defined(self) -> bool:
        return True

    @classmethod
    def get_default_storage(cls) -> Connector:
        return cls._default_storage

    @classmethod
    def set_default_storage(cls, storage: Connector) -> None:
        cls._default_storage = storage

    @staticmethod
    def get_default_child_type() -> ConnType:
        return ConnType.LocalFile

    def get_default_child_class(self) -> Class:
        child_class = self.get_default_child_type().get_class()
        return child_class

    @staticmethod
    def get_child_class_by_type(type_name: Union[ConnType, str]) -> Class:
        if isinstance(type_name, str):
            conn_type = ConnType(type_name)
        child_class = conn_type.get_class()
        return child_class

    @staticmethod
    def get_type_by_name(name: str) -> ConnType:
        if '*' in name:
            return ConnType.LocalMask
        elif '.' in name:
            return ConnType.LocalFile
        else:
            return ConnType.LocalFolder

    def get_child_class_by_name(self, name: str) -> Class:
        supposed_type = self.get_type_by_name(name)
        return self.get_child_class_by_type(supposed_type)

    def get_child_class_by_name_and_type(self, name: str, filetype: Union[ConnType, ContentType, Auto] = AUTO) -> Class:
        if Auto.is_defined(filetype):
            return ConnType(filetype).get_class()
        else:
            supposed_type = self.get_type_by_name(name)
            if supposed_type:
                return supposed_type.get_class()

    def get_files(self) -> Generator:
        for item in self.get_items():
            if hasattr(item, 'is_leaf'):
                if item.is_leaf():
                    yield item

    def file(
            self,
            name: str,
            content_format: Union[ContentFormatInterface, ContentType, Auto] = AUTO,
            filetype: Union[ContentType, Auto] = AUTO,  # deprecated argument
            **kwargs
    ) -> ConnectorInterface:
        file = self.get_children().get(name)
        if kwargs or not file:
            filename = kwargs.pop('filename', name)
            file_class = self.get_default_child_obj_class()  # LocalFile
            assert file_class, "connector class or type name aren't detected"
            if Auto.is_defined(filetype):
                if Auto.is_defined(content_format):
                    msg = 'Only one of arguments allowed: filetype (got {}) or content_format (got {})'
                    raise ValueError(msg.format(filetype, content_format))
                else:
                    msg = 'LocalFolder.file(): filetype-argument is deprecated, use content_format instead'
                    self.log(level=LoggingLevel.Warning, msg=msg, stacklevel=1)
                if isinstance(filetype, (ContentType, Auto, str)):
                    content_format = filetype
                else:  # temporary workaround for deprecated FileType class
                    content_format = filetype.get_value()
            try:
                file = file_class(filename, content_format=content_format, folder=self, **kwargs)
            except TypeError as e:
                raise TypeError('{}.{}'.format(file_class.__name__, e))
            self.add_child(file)
        return file

    def folder(self, name: str, folder_type: Union[ConnType, Auto] = AUTO, **kwargs) -> ConnectorInterface:
        if not Auto.is_defined(folder_type):
            folder_type = self.get_type_by_name(name)  # LocalFolder or LocalMask
            if folder_type == ConnType.LocalFile:
                folder_type = ConnType.LocalFolder
        if name == '..':
            return self.get_parent_folder(**kwargs)
        else:
            folder_class = ConnType(folder_type).get_class()
            folder_obj = folder_class(name, parent=self, **kwargs)
            self.add_folder(folder_obj)
            return folder_obj

    def mask(self, mask: str) -> ConnectorInterface:
        folder_type = ConnType.LocalMask
        assert isinstance(folder_type, ConnType)
        return self.folder(mask, folder_type)

    def partitioned(self, mask: str, suffix: Optional[str] = None) -> ConnectorInterface:
        folder_type = ConnType.PartitionedLocalFile
        partitioned_local_file = self.folder(mask, folder_type=folder_type)
        if suffix:
            if hasattr(partitioned_local_file, 'set_suffix'):  # isinstance(partitioned_local_file, PartitionedFile)
                partitioned_local_file.set_suffix(suffix)
            else:
                raise TypeError
        return partitioned_local_file

    def add_file(self, file: LeafConnector, inplace: bool = True) -> Optional[Native]:
        assert file.is_leaf(), 'file must be an instance of *File (got {})'.format(type(file))
        return super().add_child(file, inplace=inplace)

    def add_folder(self, folder: HierarchicFolder, inplace: bool = True):
        assert folder.is_folder()
        return super().add_child(folder, inplace=inplace)

    def set_suffix(self, suffix, inplace: bool = True):
        assert inplace, 'for LocalFolder suffixes can be set inplace only'
        for child in self.get_children().values():
            if hasattr(child, 'set_suffix'):
                child.set_suffix(suffix, inplace=True)
        return self

    def get_links(self) -> Generator:
        for item in self.get_files():
            yield from item.get_links()

    def close(self, name: Optional[str] = None) -> int:
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

    def get_parent_folder(self, skip_missing: bool = False) -> Connector:
        if self.has_parent_folder():
            return self.get_parent()
        else:
            current_folder_path = self.get_full_path()
            default_path_delimiter = self.get_path_delimiter()
            alternative_path_delimiters = '\\',
            for cur_path_delimiter in alternative_path_delimiters:
                current_folder_path = current_folder_path.replace(cur_path_delimiter, default_path_delimiter)
            path_parts = current_folder_path.split(default_path_delimiter)
            parent_parts = path_parts[:-1]
            if parent_parts:
                parent_path = default_path_delimiter.join(parent_parts)
                parent_folder = self.get_storage().folder(parent_path)
                return parent_folder
            else:
                msg = f'parent for {current_folder_path} not found'
                if skip_missing:
                    return self.log(msg, 30)
                else:
                    raise FileNotFoundError(msg)

    def is_relative_path(self) -> bool:
        return self._path_is_relative

    def has_relative_path(self) -> bool:  # path including parent folder
        if self.has_parent_folder():
            return self.get_parent().has_relative_path()
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

    def get_file_path(self, name: str) -> str:
        if self.get_folder_path():
            return self.get_folder_path() + self.get_path_delimiter() + name
        else:
            return name

    def is_existing(self) -> bool:
        return os.path.exists(self.get_path())

    def list_existing_names(self) -> list:
        if self.get_name() in ('', '.'):
            return os.listdir()
        else:
            return os.listdir(self.get_path())

    def get_existing_file_names(self) -> Generator:
        for name in self.list_existing_names():
            path = self.get_file_path(name)
            if os.path.isfile(path):
                yield name

    def get_existing_folder_names(self) -> Generator:
        for name in self.list_existing_names():
            path = self.get_file_path(name)
            if os.path.isdir(path):
                yield name

    def list_existing_file_names(self) -> Iterable:
        return list(self.get_existing_file_names())

    def all_existing_files(self, **kwargs) -> Generator:
        for name in self.list_existing_file_names():
            children = self.get_children()
            if name in children:
                yield children[name]
            else:
                yield self.file(name, **kwargs)

    def connect_all(self, inplace: bool = True, **kwargs) -> Union[list, Native]:
        files = list(self.all_existing_files(**kwargs))
        if inplace:
            return files
        else:
            return self

    @staticmethod
    def _assume_native(obj) -> Native:
        return obj


ConnType.add_classes(LocalFolder)
