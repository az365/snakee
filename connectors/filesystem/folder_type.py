try:  # Assume we're a sub-module in a package.
    from utils.enum import ClassType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.enum import ClassType


class FolderType(ClassType):
    LocalStorage = 'LocalStorage'
    LocalFolder = 'LocalFolder'
    FileMask = 'FileMask'

    # def get_name(self):
    #     return self.value
    #
    # def get_value(self):
    #     return self.value
    #
    # @staticmethod
    # def _get_dict_classes():
    #     return {FolderType.LocalFolder: LocalFolder, FolderType.FileMask: FileMask}
    #
    # def get_class(self, skip_missing=False):
    #     found_class = self._get_dict_classes().get(self)
    #     if found_class:
    #         return found_class
    #     elif not skip_missing:
    #         raise ValueError('class for {} not supported'.format(self))

    @staticmethod
    def detect_by_name(name: str):
        if '*' in name:
            return FolderType.FileMask
        else:
            return FolderType.LocalFolder

    # def get_type(self) -> ClassType:
    #     return self

    # @classmethod
    # def prepare(cls):
    #     super().prepare()
    #     cls.set_dict_classes({FolderType.LocalFolder: LocalFolder, FolderType.FileMask: FileMask})
