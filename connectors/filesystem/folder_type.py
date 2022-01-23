try:  # Assume we're a sub-module in a package.
    from base.classes.enum import ClassType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.enum import ClassType


class FolderType(ClassType):
    LocalStorage = 'LocalStorage'
    LocalFolder = 'LocalFolder'
    LocalMask = 'LocalMask'
    PartitionedLocalFile = 'PartitionedLocalFile'

    @staticmethod
    def detect_by_name(name: str):
        if '*' in name:
            return FolderType.LocalMask
        else:
            return FolderType.LocalFolder


FolderType.prepare()
