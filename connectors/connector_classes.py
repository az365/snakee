from typing import Type, Optional, Union

try:  # Assume we're a sub-module in a package.
    from interfaces import (
        ConnectorInterface, ContextInterface, Context,
        TemporaryLocationInterface, TemporaryFilesMaskInterface,
        ConnType, FolderType, FileType, Name,
    )
    from connectors.abstract.abstract_connector import AbstractConnector
    from connectors.abstract.leaf_connector import LeafConnector
    from connectors.abstract.hierarchic_connector import HierarchicConnector
    from connectors.abstract.abstract_folder import AbstractFolder, FlatFolder, HierarchicFolder
    from connectors.abstract.abstract_storage import AbstractStorage
    from connectors.filesystem.local_storage import LocalStorage
    from connectors.filesystem.local_folder import LocalFolder, FileMask
    from connectors.filesystem.local_file import AbstractFile, TextFile, JsonFile
    from connectors.filesystem.column_file import ColumnFile, CsvFile, TsvFile
    from connectors.filesystem.temporary_files import TemporaryLocation, TemporaryFilesMask
    from connectors.storages.s3_storage import AbstractObjectStorage, S3Storage
    from connectors.storages.s3_bucket import S3Bucket
    from connectors.storages.s3_folder import S3Folder
    from connectors.storages.s3_object import S3Object
    from connectors.databases.abstract_database import AbstractDatabase
    from connectors.databases.postgres_database import PostgresDatabase
    from connectors.databases.clickhouse_database import ClickhouseDatabase
    from connectors.databases.table import Table
    from connectors.operations.operation import Operation
    from connectors.operations.abstract_sync import AbstractSync
    from connectors.operations.twin_sync import TwinSync
    from connectors.operations.multi_sync import MultiSync
    from connectors.operations.job import Job
    from loggers import logger_classes as log
    from utils.decorators import deprecated_with_alternative
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..interfaces import (
        ConnectorInterface, ContextInterface, Context,
        TemporaryLocationInterface, TemporaryFilesMaskInterface,
        ConnType, FolderType, FileType, Name,
    )
    from .abstract.abstract_connector import AbstractConnector
    from .abstract.leaf_connector import LeafConnector
    from .abstract.hierarchic_connector import HierarchicConnector
    from .abstract.abstract_folder import AbstractFolder, FlatFolder, HierarchicFolder
    from .abstract.abstract_storage import AbstractStorage
    from .filesystem.local_storage import LocalStorage
    from .filesystem.local_folder import LocalFolder, FileMask
    from .filesystem.local_file import AbstractFile, TextFile, JsonFile
    from .filesystem.column_file import ColumnFile, CsvFile, TsvFile
    from .filesystem.temporary_files import TemporaryLocation, TemporaryFilesMask
    from .storages.s3_storage import AbstractObjectStorage, S3Storage
    from .storages.s3_bucket import S3Bucket
    from .storages.s3_folder import S3Folder
    from .storages.s3_object import S3Object
    from .databases.abstract_database import AbstractDatabase
    from .databases.postgres_database import PostgresDatabase
    from .databases.clickhouse_database import ClickhouseDatabase
    from .databases.table import Table
    from .operations.operation import Operation
    from .operations.abstract_sync import AbstractSync
    from .operations.twin_sync import TwinSync
    from .operations.multi_sync import MultiSync
    from .operations.job import Job
    from ..loggers import logger_classes as log
    from ..utils.decorators import deprecated_with_alternative

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
    c.get_default_file_extension.__get__(c): c for c in CONN_CLASSES
    if c in FILE_CLASSES and not c.__name__.startswith('Abstract')
}
DICT_DB_TO_DIALECT = {PostgresDatabase.__name__: 'pg', ClickhouseDatabase.__name__: 'ch'}
DB_CLASS_NAMES = DICT_DB_TO_DIALECT.keys()

_context: Context = None
_local_storage: Optional[LocalStorage] = None
PostgresDatabase.cx = _context
ConnType.set_dict_classes(DICT_CONN_CLASSES, skip_missing=True)


@deprecated_with_alternative('ConnType.get_class()')
def get_class(conn_type: Union[ConnType, Type, str]) -> Type:
    if conn_type in CONN_CLASSES:
        return conn_type
    elif isinstance(conn_type, str):
        conn_type = ConnType(conn_type)
    message = 'conn_type must be an instance of ConnType (but {} as type {} received)'
    assert isinstance(conn_type, ConnType), TypeError(message.format(conn_type, type(conn_type)))
    return conn_type.get_class()


def get_context() -> Context:
    global _context
    return _context


def set_context(cx: ContextInterface):
    global _context
    _context = cx


def is_conn(obj) -> bool:
    if hasattr(obj, 'is_connector'):
        return obj.is_connector()


def is_file(obj) -> bool:
    if isinstance(obj, AbstractFile) or obj.__class__.__name__ in FILE_CLASS_NAMES:
        return True
    elif hasattr(obj, 'is_file'):
        return obj.is_file()


def is_folder(obj) -> bool:
    if isinstance(obj, AbstractFolder) or obj.__class__.__name__ in FOLDER_CLASS_NAMES:
        return True
    elif hasattr(obj, 'is_folder'):
        return obj.is_folder()


def is_database(obj) -> bool:
    return isinstance(obj, AbstractDatabase) or obj.__class__.__name__ in DB_CLASS_NAMES


def get_dialect_type(database_type) -> Optional[str]:
    return DICT_DB_TO_DIALECT.get(ConnType(database_type).value)


def get_type_by_ext(ext, default: ConnType = ConnType.TextFile) -> ConnType:
    conn_class = DICT_EXT_TO_CLASS.get(ext)
    if conn_class:
        return ConnType(conn_class.__name__)
    else:
        return default


def get_logging_context_stub() -> ContextInterface:
    return log.LoggingContextStub()


def get_local_storage(name: Name = 'filesystem') -> LocalStorage:
    global _local_storage
    if not _local_storage:
        cx = get_context()
        if not cx:
            cx = get_logging_context_stub()
        if cx:
            _local_storage = cx.get_local_storage(name)
        else:
            _local_storage = LocalStorage(name, context=get_logging_context_stub())
    assert isinstance(_local_storage, LocalStorage), 'LocalStorage expected, got {} as {}'.format(
        _local_storage, _local_storage.__class__.__name__,
    )
    return _local_storage


def get_default_job_folder(name: Name = '') -> LocalFolder:
    return get_local_storage().folder(name)
