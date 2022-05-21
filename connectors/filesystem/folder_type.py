try:  # Assume we're a submodule in a package.
    from base.classes.enum import ClassType
    from utils.decorators import deprecated_with_alternative
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.classes.enum import ClassType
    from ...utils.decorators import deprecated_with_alternative


@deprecated_with_alternative('ConnType')
class FolderType(ClassType):
    LocalStorage = 'LocalStorage'
    LocalFolder = 'LocalFolder'
    LocalMask = 'LocalMask'
    PartitionedLocalFile = 'PartitionedLocalFile'

    @staticmethod
    @deprecated_with_alternative('ConnType.detect_by_name()')
    def detect_by_name(name: str):
        if '*' in name:
            return FolderType.LocalMask
        else:
            return FolderType.LocalFolder


FolderType.prepare()
