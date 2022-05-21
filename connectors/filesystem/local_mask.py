from typing import Iterable
import fnmatch

try:  # Assume we're a submodule in a package.
    from interfaces import ConnType, AUTO, Auto, AutoBool, AutoContext
    from connectors.abstract.hierarchic_connector import HierarchicConnector
    from connectors.abstract.abstract_folder import HierarchicFolder
    from connectors.filesystem.local_folder import LocalFolder
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import ConnType, AUTO, Auto, AutoBool, AutoContext
    from ..abstract.hierarchic_connector import HierarchicConnector
    from ..abstract.abstract_folder import HierarchicFolder
    from .local_folder import LocalFolder


class LocalMask(LocalFolder):
    def __init__(
            self,
            mask: str,
            parent: HierarchicConnector,
            context: AutoContext = None,
            verbose: AutoBool = AUTO,
    ):
        if not Auto.is_defined(parent):
            if Auto.is_defined(context):
                parent = context.get_local_storage()
        assert parent.is_folder() or parent.is_storage()
        super().__init__(path=mask, parent=parent, context=context, verbose=verbose)

    def get_mask(self) -> str:
        return self.get_name()

    def get_folder(self, skip_missing: bool = False) -> HierarchicFolder:
        parent = self.get_parent()
        if not skip_missing:
            assert isinstance(parent, HierarchicFolder)
        return parent

    def get_folder_path(self) -> str:
        return self.get_folder().get_path()

    def get_mask_path(self) -> str:
        return self.get_folder_path() + self.get_path_delimiter() + self.get_mask()

    def get_path(self, with_mask: bool = True) -> str:
        if with_mask:
            return self.get_mask_path()
        else:
            return self.get_folder_path()

    def yield_existing_names(self) -> Iterable:
        for name in self.get_folder().list_existing_names():
            if fnmatch.fnmatch(name, self.get_mask()):
                yield name

    def list_existing_names(self) -> list:
        return list(self.yield_existing_names())


ConnType.add_classes(LocalMask)
