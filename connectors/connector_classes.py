from enum import Enum

try:  # Assume we're a sub-module in a package.
    from connectors.filesystem.local_storage import (
        LocalStorage,
        LocalFolder,
        AbstractFile,
        TextFile,
        JsonFile,
        CsvFile,
        TsvFile,
    )
    from connectors.storages.s3_storage import (
        AbstractObjectStorage,
        S3Storage,
        S3Bucket,
        S3Folder,
        S3Object,
    )
    from connectors.databases.abstract_database import AbstractDatabase
    from connectors.databases.posrgres_database import PostgresDatabase
    from connectors.databases.clickhouse_database import ClickhouseDatabase
    from connectors.databases.table import Table
    from connectors.sync.twin_sync import TwinSync
    from loggers.logger_classes import deprecated, deprecated_with_alternative
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .filesystem.local_storage import (
        LocalStorage,
        LocalFolder,
        AbstractFile,
        TextFile,
        JsonFile,
        CsvFile,
        TsvFile,
    )
    from .storages.s3_storage import (
        AbstractObjectStorage,
        S3Storage,
        S3Bucket,
        S3Folder,
        S3Object,
    )
    from .databases.abstract_database import AbstractDatabase
    from .databases.posrgres_database import PostgresDatabase
    from .databases.clickhouse_database import ClickhouseDatabase
    from .databases.table import Table
    from .sync.twin_sync import TwinSync
    from ..loggers.logger_classes import deprecated_with_alternative


CONN_CLASSES = (
    LocalStorage, LocalFolder, AbstractFile,
    TextFile, JsonFile, CsvFile, TsvFile,
    AbstractDatabase, Table,
    PostgresDatabase, ClickhouseDatabase,
    # TwinSync,
)
DATABASE_TYPES = [PostgresDatabase.__class__.__name__, ClickhouseDatabase.__class__.__name__]
DICT_EXT_TO_TYPE = {'txt': TextFile, 'json': JsonFile, 'csv': CsvFile, 'tsv': TsvFile}


class ConnType(Enum):
    LocalStorage = 'LocalStorage'
    LocalFolder = 'LocalFolder'
    TextFile = 'TextFile'
    JsonFile = 'JsonFile'
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

    def get_class(self):
        if self == ConnType.LocalStorage:
            return LocalStorage
        elif self == ConnType.LocalFolder:
            return LocalFolder
        elif self == ConnType.TextFile:
            return TextFile
        elif self == ConnType.JsonFile:
            return JsonFile
        elif self == ConnType.CsvFile:
            return CsvFile
        elif self == ConnType.TsvFile:
            return CsvFile
        elif self == ConnType.S3Storage:
            return S3Storage
        elif self == ConnType.S3Bucket:
            return S3Bucket
        elif self == ConnType.S3Folder:
            return S3Folder
        elif self == ConnType.S3Object:
            return S3Object
        elif self == ConnType.PostgresDatabase:
            return PostgresDatabase
        elif self == ConnType.ClickhouseDatabase:
            return ClickhouseDatabase
        elif self == ConnType.Table:
            return Table
        elif self == ConnType.TwinSync:
            return TwinSync


@deprecated_with_alternative('ConnType.get_class()')
def get_class(conn_type):
    if conn_type in CONN_CLASSES:
        return conn_type
    elif isinstance(conn_type, str):
        conn_type = ConnType(conn_type)
    message = 'conn_type must be an instance of ConnType (but {} as type {} received)'
    assert isinstance(conn_type, ConnType), TypeError(message.format(conn_type, type(conn_type)))
    return conn_type.get_class()


def is_conn(obj):
    return isinstance(obj, CONN_CLASSES)


def is_file(obj):
    return isinstance(obj, (TextFile, JsonFile, CsvFile, TsvFile))


def is_folder(obj):
    return obj.__class__.__name__ == 'LocalFolder'


def is_database(obj):
    return obj.__class__.__name__ in DATABASE_TYPES
