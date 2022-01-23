from typing import Type, Optional, Union

try:  # Assume we're a submodule in a package.
    from interfaces import (
        ConnectorInterface, ContextInterface, Context,
        TemporaryLocationInterface, TemporaryFilesMaskInterface,
        ConnType, FolderType, Name, DialectType,
    )
    from utils.decorators import deprecated_with_alternative
    from loggers import logger_classes as log
    from content.format.format_classes import (
        AbstractFormat, BinaryFormat, ParsedFormat, LeanFormat,
        TextFormat, JsonFormat, ColumnarFormat, FlatStructFormat,
        ContentType,
    )
    from connectors.abstract.abstract_connector import AbstractConnector
    from connectors.abstract.leaf_connector import LeafConnector
    from connectors.abstract.hierarchic_connector import HierarchicConnector
    from connectors.abstract.abstract_folder import AbstractFolder, FlatFolder, HierarchicFolder
    from connectors.abstract.abstract_storage import AbstractStorage
    from connectors.mixin.connector_format_mixin import ConnectorFormatMixin
    from connectors.mixin.actualize_mixin import ActualizeMixin
    from connectors.mixin.streamable_mixin import StreamableMixin
    from connectors.filesystem.local_storage import LocalStorage
    from connectors.filesystem.local_folder import LocalFolder
    from connectors.filesystem.local_file import LocalFile
    from connectors.filesystem.local_mask import LocalMask
    from connectors.filesystem.partitioned_local_file import PartitionedLocalFile
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
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..interfaces import (
        ConnectorInterface, ContextInterface, Context,
        TemporaryLocationInterface, TemporaryFilesMaskInterface,
        ConnType, FolderType, Name, DialectType,
    )
    from ..utils.decorators import deprecated_with_alternative
    from ..loggers import logger_classes as log
    from ..content.format.format_classes import (
        AbstractFormat, BinaryFormat, ParsedFormat, LeanFormat,
        TextFormat, JsonFormat, ColumnarFormat, FlatStructFormat,
        ContentType,
    )
    from .abstract.abstract_connector import AbstractConnector
    from .abstract.leaf_connector import LeafConnector
    from .abstract.hierarchic_connector import HierarchicConnector
    from .abstract.abstract_folder import AbstractFolder, FlatFolder, HierarchicFolder
    from .abstract.abstract_storage import AbstractStorage
    from .mixin.connector_format_mixin import ConnectorFormatMixin
    from .mixin.actualize_mixin import ActualizeMixin
    from .mixin.streamable_mixin import StreamableMixin
    from .filesystem.local_storage import LocalStorage
    from .filesystem.local_folder import LocalFolder
    from .filesystem.local_file import LocalFile
    from .filesystem.temporary_files import TemporaryLocation, TemporaryFilesMask
    from .filesystem.local_mask import LocalMask
    from .filesystem.partitioned_local_file import PartitionedLocalFile
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

CONN_CLASSES = (
    AbstractConnector,
    LeafConnector,
    HierarchicConnector,
    AbstractFolder, FlatFolder, HierarchicFolder,
    AbstractStorage,
    LocalStorage,
    LocalFolder, LocalMask, LocalFile, PartitionedLocalFile,
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
FOLDER_CLASSES = (LocalFolder, LocalMask, PartitionedLocalFile, S3Folder)
FOLDER_CLASS_NAMES = tuple([c.__name__ for c in FOLDER_CLASSES])
FILE_CLASSES = tuple([c for c in CONN_CLASSES if c.__name__.endswith('File')])
FILE_CLASS_NAMES = tuple([c.__name__ for c in FILE_CLASSES])
DICT_EXT_TO_CLASS = {
    c.get_default_file_extension.__get__(c): c for c in CONN_CLASSES
    if c in FILE_CLASSES and not c.__name__.startswith('Abstract')
}
DICT_DB_TO_DIALECT_TYPE = {
    PostgresDatabase.__name__: DialectType.Postgres,
    ClickhouseDatabase.__name__: DialectType.Clickhouse,
}
DB_CLASS_NAMES = DICT_DB_TO_DIALECT_TYPE.keys()

_context: Context = None
_local_storage: Optional[LocalStorage] = None
PostgresDatabase.cx = _context
ConnType.set_dict_classes(DICT_CONN_CLASSES, skip_missing=True)

AbstractStorage.set_parent_obj_classes([ContextInterface])
AbstractDatabase.set_child_obj_classes([Table])
Table.set_parent_obj_classes([AbstractDatabase, PostgresDatabase, ClickhouseDatabase])
LocalStorage.set_child_obj_classes([LocalFolder, LocalMask, LocalFile, PartitionedLocalFile])
LocalFolder.set_parent_obj_classes([LocalStorage, LocalFolder])
LocalFolder.set_child_obj_classes([LocalFile, PartitionedLocalFile, LocalMask, LocalFolder, LocalStorage])
S3Storage.set_child_obj_classes([S3Bucket])
S3Bucket.set_parent_obj_classes([S3Storage])
S3Bucket.set_child_obj_classes([S3Folder, S3Object])
S3Folder.set_parent_obj_classes([S3Bucket, S3Folder])
S3Folder.set_child_obj_classes([S3Folder, S3Object])
S3Object.set_parent_obj_classes([S3Folder, S3Bucket])


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
    AbstractStorage.set_default_context(cx)
    AbstractStorage.set_parent_obj_classes([cx.__class__])
    default_folder = cx.get_job_folder()
    assert isinstance(default_folder, LocalFolder)
    LocalFile.set_default_folder(default_folder)


def is_conn(obj) -> bool:
    if hasattr(obj, 'is_connector'):
        return obj.is_connector()


def is_file(obj) -> bool:
    if isinstance(obj, LocalFile) or obj.__class__.__name__ in FILE_CLASS_NAMES:
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


def get_dialect_type(database_type) -> Optional[DialectType]:
    conn_type_name = ConnType(database_type).get_value()
    return DICT_DB_TO_DIALECT_TYPE.get(conn_type_name)


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
