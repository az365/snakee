try:  # Assume we're a sub-module in a package.
    from utils.enum import ClassType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils.enum import ClassType


class ConnType(ClassType):
    # Only concrete classes, not abstract ones
    LocalStorage = 'LocalStorage'
    LocalFolder = 'LocalFolder'
    FileMask = 'FileMask'
    TextFile = 'TextFile'
    JsonFile = 'JsonFile'
    ColumnFile = 'ColumnFile'
    CsvFile = 'CsvFile'
    TsvFile = 'TsvFile'
    PostgresDatabase = 'PostgresDatabase'
    ClickhouseDatabase = 'ClickhouseDatabase'
    Table = 'Table'
    S3Storage = 'S3Storage'
    S3Bucket = 'S3Bucket'
    S3Folder = 'S3Folder'
    S3Object = 'S3Object'
    TwinSync = 'TwinSync'


ConnType.prepare()
