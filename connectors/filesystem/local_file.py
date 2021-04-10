from abc import ABC, abstractmethod
from typing import Optional, Union, Iterable, Callable, NoReturn
from inspect import isclass
import os
import gzip as gz

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from items import base_item_type as it
    from base.interfaces.context_interface import ContextInterface
    from connectors.abstract.connector_interface import ConnectorInterface
    from connectors.abstract.leaf_connector import LeafConnector
    from streams.interfaces.regular_stream_interface import RegularStreamInterface
    from streams.mixin.stream_builder_mixin import StreamBuilderMixin
    from streams import stream_classes as sm
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...items import base_item_type as it
    from ...base.interfaces.context_interface import ContextInterface
    from ..abstract.connector_interface import ConnectorInterface
    from ..abstract.leaf_connector import LeafConnector
    from ...streams.interfaces.regular_stream_interface import RegularStreamInterface
    from ...streams.mixin.stream_builder_mixin import StreamBuilderMixin
    from ...streams import stream_classes as sm

OptionalFields = Optional[Union[str, Iterable]]
Context = Optional[ContextInterface]
Folder = ConnectorInterface
Stream = RegularStreamInterface

AUTO = arg.DEFAULT
CHUNK_SIZE = 8192


class AbstractFile(LeafConnector, StreamBuilderMixin, ABC):
    def __init__(
            self,
            name: str,
            folder: Optional[Folder] = None,
            context: Union[Context, arg.DefaultArgument] = arg.DEFAULT,
            verbose: Union[bool, arg.DefaultArgument] = AUTO,
    ):
        if folder:
            message = 'only LocalFolder supported for *File instances (got {})'.format(type(folder))
            assert isinstance(folder, Folder), message
            assert folder.is_folder(), message
        super().__init__(name=name, parent=folder)
        self._fileholder = None
        self.verbose = arg.undefault(verbose, self.get_folder().verbose if self.get_folder() else True)

    def get_folder(self):
        return self.get_parent()

    def get_links(self):
        return self._data

    def get_fileholder(self):
        return self._fileholder

    def set_fileholder(self, fileholder):
        self._fileholder = fileholder

    def add_to_folder(self, folder: Folder):
        assert isinstance(folder, Folder), 'folder must be a LocalFolder (got {})'.format(type(folder))
        assert folder.is_folder(), 'folder must be a LocalFolder (got {})'.format(type(folder))
        folder.add_child(self)

    @staticmethod
    def get_default_file_extension() -> str:
        pass

    @staticmethod
    def get_default_item_type() -> it.ItemType:
        return it.ItemType.Any

    @classmethod
    def get_stream_type(cls):
        return sm.StreamType.AnyStream

    @classmethod
    def get_stream_class(cls):
        return sm.get_class(cls.get_stream_type())

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

    def is_inside_folder(self, folder: Union[str, Folder, arg.DefaultArgument] = AUTO) -> bool:
        folder_obj = arg.undefault(folder, self.get_folder())
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

    def is_closed(self):
        fileholder = self.get_fileholder()
        if hasattr(fileholder, 'closed'):
            return fileholder.closed

    def close(self) -> int:
        if self.is_opened():
            self.get_fileholder().close()
            return 1

    def open(self, mode='r', reopen=False):
        if self.is_opened():
            if reopen:
                self.close()
            else:
                raise AttributeError('File {} is already opened'.format(self.get_name()))
        else:
            fileholder = open(self.get_path(), 'r')
            self.set_fileholder(fileholder)

    def remove(self, log=True) -> int:
        file_path = self.get_path()
        if log:
            self.get_logger().log('Trying remove {}...'.format(file_path), level=20)
        os.remove(file_path)
        if log:
            self.get_logger().log('Successfully removed {}.'.format(file_path), level=20)
        return 1

    def is_existing(self) -> bool:
        return os.path.exists(self.get_path())

    def _get_generated_stream_name(self):
        return arg.get_generated_name('{}:stream'.format(self.get_name()), include_random=True, include_datetime=False)

    @abstractmethod
    def from_stream(self, stream):
        pass

    def to_stream(
            self,
            data: Union[Iterable, arg.DefaultArgument] = arg.DEFAULT,
            name: Union[str, arg.DefaultArgument] = arg.DEFAULT,
            stream_type=arg.DEFAULT,
            ex: OptionalFields = None,
            **kwargs
    ) -> Stream:
        stream_type = arg.delayed_undefault(stream_type, self.get_stream_type)
        name = arg.delayed_undefault(name, self._get_generated_stream_name)
        if not arg.is_defined(data):
            data = self.get_data()
        if isinstance(stream_type, str):
            stream_class = sm.StreamType(stream_type).get_class()
        elif isclass(stream_type):
            stream_class = stream_type
        else:
            stream_class = stream_type.get_class()
        meta = self.get_compatible_meta(stream_class, name=name, ex=ex, **kwargs)
        if 'count' not in meta:
            meta['count'] = self.get_count()
        if 'source' not in meta:
            meta['source'] = self
        return stream_class(data, **meta)

    def collect(self, **kwargs) -> Stream:
        return self.to_stream(**kwargs).collect()

    @abstractmethod
    def get_count(self):
        pass

    def stream(
            self,
            data: Union[Iterable, arg.DefaultArgument] = arg.DEFAULT,
            stream_type=arg.DEFAULT,
            ex: OptionalFields = None,
            **kwargs
    ) -> Stream:
        return self.to_stream(data, stream_type, ex, **kwargs)

    def map(self, function: Callable) -> Stream:
        return self.stream(
            map(function, self.get_items()),
        )

    def filter(self, function: Callable) -> Stream:
        return self.stream(
            filter(function, self.get_items()),
            count=None,
        )

    def is_in_memory(self) -> bool:
        return False

    @staticmethod
    def is_file() -> bool:
        return True


class TextFile(AbstractFile):
    def __init__(
            self,
            name,
            gzip=False,
            encoding='utf8',
            end='\n',
            expected_count=AUTO,
            folder=None,
            verbose=AUTO,
    ):
        self.gzip = gzip
        self.encoding = encoding
        self.end = end
        self.count = expected_count
        super().__init__(
            name=name,
            folder=folder,
            verbose=verbose,
        )

    @staticmethod
    def get_default_file_extension() -> str:
        return 'txt'

    @staticmethod
    def get_default_item_type() -> it.ItemType:
        return it.ItemType.Line

    @classmethod
    def get_stream_type(cls):
        return sm.StreamType.LineStream

    def get_data(self, verbose=arg.DEFAULT, *args, **kwargs) -> Iterable:
        return self.get_items(verbose=verbose, *args, **kwargs)

    def open(self, mode='r', reopen=False) -> NoReturn:
        if self.is_opened():
            if reopen:
                self.close()
            else:
                raise AttributeError('File {} is already opened'.format(self.get_name()))
        if self.gzip:
            fileholder = gz.open(self.get_path(), mode)
            self.set_fileholder(fileholder)
        else:
            params = dict()
            if self.encoding:
                params['encoding'] = self.encoding
            fileholder = open(self.get_path(), mode, **params) if self.encoding else open(self.get_path(), 'r')
            self.set_fileholder(fileholder)

    def count_lines(self, reopen=False, chunk_size=CHUNK_SIZE, verbose=AUTO) -> int:
        verbose = arg.undefault(verbose, self.verbose)
        self.log('Counting lines in {}...'.format(self.get_name()), end='\r', verbose=verbose)
        self.open(reopen=reopen)
        count_n = sum(chunk.count('\n') for chunk in iter(lambda: self.get_fileholder().read(chunk_size), ''))
        self.count = count_n + 1
        self.close()
        self.log('Detected {} lines in {}.'.format(self.count, self.get_name()), end='\r', verbose=verbose)
        return self.count

    def get_count(self, reopen=True) -> Optional[int]:
        if (self.count is None or self.count == AUTO) and not self.gzip:
            self.count = self.count_lines(reopen=reopen)
        return self.count

    def get_next_lines(self, count=None, skip_first=False, close=False) -> Iterable:
        assert self.is_opened()
        for n, row in enumerate(self.get_fileholder()):
            if skip_first and n == 0:
                continue
            if isinstance(row, bytes):
                row = row.decode(self.encoding) if self.encoding else row.decode()
            if self.end:
                row = row.rstrip(self.end)
            yield row
            if (count or 0) > 0 and (n + 1 == count):
                break
        if close:
            self.close()

    def get_lines(self, count=None, skip_first=False, check=True, verbose=AUTO, step=AUTO) -> Iterable:
        if check and not self.gzip:
            assert self.get_count(reopen=True) > 0
        self.open(reopen=True)
        lines = self.get_next_lines(count=count, skip_first=skip_first, close=True)
        verbose = arg.undefault(verbose, self.verbose)
        if verbose:
            message = verbose if isinstance(verbose, str) else 'Reading {}'.format(self.get_name())
            logger = self.get_logger()
            assert hasattr(logger, 'progress'), '{} has no progress in {}'.format(self, logger)
            lines = self.get_logger().progress(lines, name=message, count=self.count, step=step)
        return lines

    def get_items(self, verbose=AUTO, step=AUTO) -> Iterable:
        verbose = arg.undefault(verbose, self.verbose)
        if isinstance(verbose, str):
            self.log(verbose, verbose=verbose)
        elif (self.get_count() or 0) > 0:
            self.log('Expecting {} lines in file {}...'.format(self.get_count(), self.get_name()), verbose=verbose)
        return self.get_lines(verbose=verbose, step=step)

    def get_stream(self, to=AUTO, verbose=AUTO) -> Stream:
        to = arg.undefault(to, self.get_stream_type())
        return self.to_stream_class(
            stream_class=sm.get_class(to),
            verbose=verbose,
        )

    def get_stream_kwargs(self, data=AUTO, name=AUTO, verbose=AUTO, step=AUTO, **kwargs) -> dict:
        verbose = arg.undefault(verbose, self.verbose)
        data = arg.delayed_undefault(data, self.get_items, verbose=verbose, step=step)
        name = arg.delayed_undefault(name, self._get_generated_stream_name)
        result = dict(
            data=data,
            name=name,
            source=self,
            count=self.get_count(),
            context=self.get_context(),
        )
        result.update(kwargs)
        return result

    def to_stream_class(self, stream_class, **kwargs) -> Stream:
        return stream_class(
            **self.get_stream_kwargs(**kwargs)
        )

    def to_line_stream(self, step=AUTO, verbose=AUTO, **kwargs) -> Stream:
        data = self.get_lines(step=step, verbose=verbose)
        stream_kwargs = self.get_stream_kwargs(data=data, step=step, verbose=verbose, **kwargs)
        return sm.LineStream(**stream_kwargs)

    def to_any_stream(self, **kwargs) -> Stream:
        return sm.AnyStream(
            **self.get_stream_kwargs(**kwargs)
        )

    def write_lines(self, lines, verbose=AUTO) -> NoReturn:
        verbose = arg.undefault(verbose, self.verbose)
        self.open('w', reopen=True)
        n = 0
        for n, i in enumerate(lines):
            if n > 0:
                self.get_fileholder().write(self.end.encode(self.encoding) if self.gzip else self.end)
            self.get_fileholder().write(str(i).encode(self.encoding) if self.gzip else str(i))
        self.close()
        self.log('Done. {} rows has written into {}'.format(n + 1, self.get_name()), verbose=verbose)

    def write_stream(self, stream: RegularStreamInterface, verbose=AUTO):
        assert sm.is_stream(stream)
        return self.write_lines(
            stream.to_line_stream().get_data(),
            verbose=verbose,
        )

    def from_stream(self, stream: RegularStreamInterface):
        assert isinstance(stream, sm.LineStream)
        return self.write_lines(stream.get_iter())

    def skip(self, count: int) -> RegularStreamInterface:
        return self.to_line_stream().skip(count)

    def take(self, count: int) -> RegularStreamInterface:
        return self.to_line_stream().take(count)

    def apply_to_data(self, function, *args, dynamic=False, **kwargs):  # ?
        return self.stream(  # can be file
            function(self.get_data(), *args, **kwargs),
        ).set_meta(
            **self.get_static_meta() if dynamic else self.get_meta()
        )


class JsonFile(TextFile):
    def __init__(
            self,
            name,
            encoding='utf8',
            gzip=False,
            expected_count=AUTO,
            schema=AUTO,
            default_value=None,
            folder=None,
            verbose=AUTO,
    ):
        super().__init__(
            name=name,
            encoding=encoding,
            gzip=gzip,
            expected_count=expected_count,
            folder=folder,
            verbose=verbose,
        )
        self.schema = schema
        self.default_value = default_value

    @staticmethod
    def get_default_file_extension() -> str:
        return 'json'

    @staticmethod
    def get_default_item_type() -> it.ItemType:
        return it.ItemType.Any

    @classmethod
    def get_stream_type(cls):
        return sm.StreamType.AnyStream

    def get_items(self, verbose=AUTO, step=AUTO) -> Iterable:
        return self.to_line_stream(
            verbose=verbose,
        ).parse_json(
            default_value=self.default_value,
        ).get_items()

    def to_record_stream(self, verbose=AUTO, **kwargs) -> Stream:
        return sm.RecordStream(
            self.get_items(verbose=verbose),
            count=self.get_count(),
            source=self,
            context=self.get_context(),
            **kwargs
        )

    def write_stream(self, stream, verbose=AUTO):
        assert sm.is_stream(stream)
        return self.write_lines(
            stream.to_json().get_data(),
            verbose=verbose,
        )
