from enum import Enum

try:  # Assume we're a sub-module in a package.
    from connectors.abstract.abstract_connector import AbstractConnector
    from connectors.abstract.leaf_connector import LeafConnector
    from connectors.abstract.hierarchic_connector import HierarchicConnector
    from connectors.abstract.abstract_folder import AbstractFolder, FlatFolder, HierarchicFolder
    from connectors.abstract.abstract_storage import AbstractStorage
    from connectors.filesystem.local_storage import LocalStorage
    from connectors.filesystem.local_folder import LocalFolder, FileMask
    from connectors.filesystem.local_file import AbstractFile, TextFile, JsonFile, ColumnFile, CsvFile, TsvFile
    from connectors.storages.s3_storage import AbstractObjectStorage, S3Storage
    from connectors.storages.s3_bucket import S3Bucket, S3Folder
    from connectors.storages.s3_object import S3Object
    from connectors.databases.abstract_database import AbstractDatabase
    from connectors.databases.posrgres_database import PostgresDatabase
    from connectors.databases.clickhouse_database import ClickhouseDatabase
    from connectors.databases.table import Table
    from connectors.sync.twin_sync import TwinSync
    from loggers.logger_classes import deprecated, deprecated_with_alternative
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .abstract.abstract_connector import AbstractConnector
    from .abstract.leaf_connector import LeafConnector
    from .abstract.hierarchic_connector import HierarchicConnector
    from .abstract.abstract_folder import AbstractFolder, FlatFolder, HierarchicFolder
    from .abstract.abstract_storage import AbstractStorage
    from .filesystem.local_storage import LocalStorage
    from .filesystem.local_folder import LocalFolder, FileMask
    from .filesystem.local_file import AbstractFile, TextFile, JsonFile, ColumnFile, CsvFile, TsvFile
    from .storages.s3_storage import AbstractObjectStorage, S3Storage
    from .storages.s3_bucket import S3Bucket, S3Folder
    from .storages.s3_object import S3Object
    from .databases.abstract_database import AbstractDatabase
    from .databases.posrgres_database import PostgresDatabase
    from .databases.clickhouse_database import ClickhouseDatabase
    from .databases.table import Table
    from .sync.twin_sync import TwinSync
    from ..loggers.logger_classes import deprecated_with_alternative


CONN_CLASSES = (
    AbstractConnector,
    LeafConnector,
    HierarchicConnector,
    AbstractFolder, FlatFolder, HierarchicFolder,
    AbstractStorage,
    LocalStorage,
    LocalFolder, FileMask,
    AbstractFile, TextFile, JsonFile, ColumnFile, CsvFile, TsvFile,
    AbstractObjectStorage, S3Storage,
    S3Bucket, S3Folder,
    S3Object,
    AbstractDatabase,
    PostgresDatabase,
    ClickhouseDatabase,
    Table,
    TwinSync,
)
DICT_CONN_CLASSES = {c.__name__: c for c in CONN_CLASSES}
FOLDER_CLASSES = (LocalFolder, FileMask, S3Folder)
FOLDER_CLASS_NAMES = tuple([c.__name__ for c in FOLDER_CLASSES])
FILE_CLASSES = tuple([c for c in CONN_CLASSES if c.__name__.endswith('File')])
FILE_CLASS_NAMES = tuple([c.__name__ for c in FILE_CLASSES])
DICT_EXT_TO_CLASS = {
    c.get_default_file_extension(): c for c in CONN_CLASSES
    if c in FILE_CLASSES and not c.__name__.startswith('Abstract')
}
DICT_DB_TO_DIALECT = {PostgresDatabase.__name__: 'pg', ClickhouseDatabase.__name__: 'ch'}
DB_CLASS_NAMES = DICT_DB_TO_DIALECT.keys()


class ConnType(Enum):
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

    def get_class(self):
        return DICT_CONN_CLASSES[self.value]


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
    return isinstance(obj, AbstractFile) or obj.__class__.__name__ in FILE_CLASS_NAMES


def is_folder(obj):
    return isinstance(obj, AbstractFolder) or obj.__class__.__name__ in FOLDER_CLASS_NAMES


def is_database(obj):
    return isinstance(obj, AbstractDatabase) or obj.__class__.__name__ in DB_CLASS_NAMES


def get_dialect_type(database_type):
    return DICT_DB_TO_DIALECT.get(ConnType(database_type).value)


def get_type_by_ext(ext, default=ConnType.TextFile):
    conn_class = DICT_EXT_TO_CLASS.get(ext)
    if conn_class:
        return ConnType(conn_class.__name__)
    else:
        return default
