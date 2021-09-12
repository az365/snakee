try:  # Assume we're a sub-module in a package.
    from utils.enum import ClassType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.enum import ClassType


class FileType(ClassType):
    TextFile = 'TextFile'
    JsonFile = 'JsonFile'
    ColumnFile = 'ColumnFile'
    CsvFile = 'CsvFile'
    TsvFile = 'TsvFile'

    @staticmethod
    def _get_dict_ext():
        return {
            'txt': FileType.TextFile,
            'json': FileType.JsonFile,
            'csv': FileType.CsvFile,
            'tsv': FileType.TsvFile,
        }

    @classmethod
    def detect_by_ext(cls, ext: str):
        return cls._get_dict_ext().get(ext)

    @classmethod
    def detect_by_name(cls, name: str):
        ext = name.split('.')[-1]
        return cls.detect_by_ext(ext)


FileType.prepare()
