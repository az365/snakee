from enum import Enum

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from connectors.filesystem.local_file import TextFile, JsonFile
    from connectors.filesystem.column_file import ColumnFile, CsvFile, TsvFile
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .local_file import TextFile, JsonFile
    from .column_file import ColumnFile, CsvFile, TsvFile


class FileType(Enum):
    TextFile = 'TextFile'
    JsonFile = 'JsonFile'
    ColumnFile = 'ColumnFile'
    CsvFile = 'CsvFile'
    TsvFile = 'TsvFile'

    def get_name(self):
        return self.value

    def get_value(self):
        return self.value

    @staticmethod
    def _get_dict_classes():
        return {
            FileType.TextFile: TextFile,
            FileType.JsonFile: JsonFile,
            FileType.ColumnFile: ColumnFile,
            FileType.CsvFile: CsvFile,
            FileType.TsvFile: TsvFile,
        }

    def get_class(self, skip_missing=False):
        found_class = self._get_dict_classes().get(self)
        if found_class:
            return found_class
        elif not skip_missing:
            raise ValueError('class for {} not supported'.format(self))

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