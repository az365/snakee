from abc import ABC, abstractmethod
from typing import Optional, Iterable, Union, Any
import os

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from utils.decorators import deprecated_with_alternative
    from interfaces import (
        Context, Connector, ConnectorInterface, IterableStreamInterface, ItemType, StreamType,
        AUTO, Auto, AutoName, AutoCount, AutoBool, AutoConnector, OptionalFields,
    )
    from functions.primary import dates as dt
    from connectors.abstract.leaf_connector import LeafConnector
    from connectors.mixin.streamable_mixin import StreamableMixin
    from streams.mixin.stream_builder_mixin import StreamBuilderMixin
    from streams.mixin.iterable_mixin import IterableStreamMixin
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...utils.decorators import deprecated_with_alternative
    from ...interfaces import (
        Context, Connector, ConnectorInterface, IterableStreamInterface, ItemType, StreamType,
        AUTO, Auto, AutoName, AutoCount, AutoBool, AutoConnector, OptionalFields,
    )
    from ...functions.primary import dates as dt
    from ..abstract.leaf_connector import LeafConnector
    from ..mixin.streamable_mixin import StreamableMixin
    from ...streams.mixin.stream_builder_mixin import StreamBuilderMixin
    from ...streams.mixin.iterable_mixin import IterableStreamMixin

Stream = IterableStreamInterface
Native = Union[LeafConnector, Stream]

CHUNK_SIZE = 8192
LOGGING_LEVEL_INFO = 20
LOGGING_LEVEL_WARN = 30


class AbstractFile(LeafConnector, StreamableMixin, ABC):
    @deprecated_with_alternative('LocalFile')
    def __init__(self, name: str, folder: Connector = None, context: Context = AUTO, verbose: AutoBool = AUTO):
        if folder:
            message = 'only LocalFolder supported for *File instances (got {})'.format(type(folder))
            assert isinstance(folder, ConnectorInterface) or folder.is_folder(), message
        else:
            folder = context.get_job_folder()
        self._fileholder = None
        self._modification_ts = None
        super().__init__(name=name, parent=folder, verbose=verbose)

    def get_folder(self) -> Union[Connector, Any]:
        return self.get_parent()

    def get_links(self) -> dict:
        return self._data

    def get_fileholder(self):
        return self._fileholder

    def set_fileholder(self, fileholder, inplace: bool = False) -> Optional[Native]:
        self._fileholder = fileholder
        if not inplace:
            return self

    def add_to_folder(self, folder: Connector) -> Native:
        assert isinstance(folder, ConnectorInterface), 'folder must be a LocalFolder (got {})'.format(type(folder))
        assert folder.is_folder(), 'folder must be a LocalFolder (got {})'.format(type(folder))
        folder.add_child(self)
        return self

    @staticmethod
    def get_default_file_extension() -> str:
        pass

    def is_directly_in_parent_folder(self) -> bool:
        return self.get_path_delimiter() in self.get_name()

    def has_path_from_root(self) -> bool:
        name = self.get_name()
        if isinstance(name, str):
            return name.startswith(self.get_path_delimiter()) or ':' in name

    def get_path(self) -> str:
        if self.has_path_from_root() or not self.get_folder():
            return self.get_name()
        else:
            folder_path = self.get_folder().get_path()
            if '*' in folder_path:
                folder_path = folder_path.replace('*', '{}')
            if arg.is_formatter(folder_path):
                return folder_path.format(self.get_name())
            elif folder_path.endswith(self.get_path_delimiter()):
                return folder_path + self.get_name()
            elif folder_path:
                return '{}{}{}'.format(folder_path, self.get_path_delimiter(), self.get_name())
            else:
                return self.get_name()

    def get_list_path(self) -> Iterable:
        return self.get_path().split(self.get_path_delimiter())

    def get_folder_path(self) -> str:
        return self.get_path_delimiter().join(self.get_list_path()[:-1])

    def is_inside_folder(self, folder: Union[str, Connector, Auto] = AUTO) -> bool:
        folder_obj = arg.acquire(folder, self.get_folder())
        if isinstance(folder_obj, str):
            folder_path = folder_obj
        else:  # elif isinstance(folder_obj, LocalFolder)
            folder_path = folder_obj.get_path()
        return self.get_folder_path() in folder_path

    def is_opened(self) -> bool:
        if self.get_fileholder() is None:
            return False
        else:
            return not self.is_closed()

    def is_closed(self) -> bool:
        fileholder = self.get_fileholder()
        if hasattr(fileholder, 'closed'):
            return fileholder.closed

    def close(self) -> int:
        if self.is_opened():
            self.get_fileholder().close()
            return 1
        else:
            return 0

    def open(self, mode: str = 'r', allow_reopen: bool = False) -> Native:
        if self.is_opened():
            if allow_reopen:
                self.close()
            else:
                raise AttributeError('File {} is already opened'.format(self.get_name()))
        else:
            fileholder = open(self.get_path(), mode)
            self.set_fileholder(fileholder)
        return self

    def remove(self, log: bool = True, verbose: bool = True) -> int:
        file_path = self.get_path()
        level = LOGGING_LEVEL_WARN if verbose else LOGGING_LEVEL_INFO
        if log:
            self.get_logger().log('Trying remove {}...'.format(file_path), level=level)
        os.remove(file_path)
        if log or verbose:
            self.get_logger().log('Successfully removed {}.'.format(file_path), level=level)
        return 1

    def is_existing(self) -> bool:
        return os.path.exists(self.get_path())

    def is_changed_by_another(self) -> bool:
        return not self.is_actual()

    def is_actual(self) -> bool:
        return self.get_modification_timestamp() == self.get_prev_modification_timestamp()

    def actualize(self) -> Native:
        self.get_modification_timestamp()
        self.get_count(force=True)
        return self

    def get_modification_time_str(self) -> str:
        timestamp = dt.datetime.fromtimestamp(self.get_modification_timestamp())
        return dt.get_formatted_datetime(timestamp)

    def get_prev_modification_timestamp(self) -> Optional[float]:
        return self._modification_ts

    def get_modification_timestamp(self, reset: bool = True) -> Optional[float]:
        if self.is_existing():
            timestamp = os.path.getmtime(self.get_path())
            if reset or not self.get_prev_modification_timestamp():
                self._modification_ts = timestamp
            return timestamp

    def reset_modification_timestamp(self, timestamp: Union[float, Auto, None] = AUTO) -> Native:
        timestamp = arg.acquire(timestamp, self.get_modification_timestamp(reset=False))
        self._modification_ts = timestamp
        return self

    def get_file_age_str(self):
        timestamp = self.get_modification_timestamp()
        if timestamp:
            timedelta_age = dt.datetime.now() - dt.datetime.fromtimestamp(timestamp)
            assert isinstance(timedelta_age, dt.timedelta)
            if timedelta_age.seconds == 0:
                return 'now'
            elif timedelta_age.seconds > 0:
                return dt.get_str_from_timedelta(timedelta_age)
            else:
                return 'future'

    def get_datetime_str(self) -> str:
        if self.is_existing():
            times = self.get_modification_time_str(), self.get_file_age_str(), dt.get_current_time_str()
            return '{} + {} = {}'.format(*times)
        else:
            return dt.get_current_time_str()

    @abstractmethod
    def get_count(self, allow_reopen: bool = True, allow_slow_gzip: bool = True, force: bool = False) -> Optional[int]:
        pass

    def is_empty(self) -> Optional[bool]:
        count = self.get_count(allow_slow_gzip=False)
        if count is not None:
            return count <= 0

    def is_in_memory(self) -> bool:
        return False

    @staticmethod
    def is_file() -> bool:
        return True

    def is_verbose(self) -> bool:
        return self.verbose

    def get_children(self) -> dict:
        return self._data
