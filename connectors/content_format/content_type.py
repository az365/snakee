from typing import Optional

try:  # Assume we're a sub-module in a package.
    from base.enum import ClassType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.enum import ClassType


class ContentType(ClassType):
    TextFile = 'TextFile'
    JsonFile = 'JsonFile'
    ColumnFile = 'ColumnFile'
    CsvFile = 'CsvFile'
    TsvFile = 'TsvFile'

    @staticmethod
    def _get_dict_extensions():
        return {
            'txt': ContentType.TextFile,
            'json': ContentType.JsonFile,
            'csv': ContentType.CsvFile,
            'tsv': ContentType.TsvFile,
        }

    @classmethod
    def detect_by_file_extension(cls, ext: str):
        return cls._get_dict_extensions().get(ext)

    @classmethod
    def detect_by_name(cls, name: str):
        ext = name.split('.')[-1]
        return cls.detect_by_file_extension(ext)

    def get_default_file_extension(self) -> Optional[str]:
        for ext, file_type in self._get_dict_extensions().items():
            if file_type == self:
                return ext


ContentType.prepare()
