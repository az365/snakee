from abc import ABC, abstractmethod
from typing import Optional, Iterable, Callable, Union, Any
from inspect import isclass
import os
import gzip as gz

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg, dates as dt
    from interfaces import (
        Context, Connector, ConnectorInterface, IterableStreamInterface, ItemType, StreamType,
        AUTO, Auto, AutoName, AutoCount, AutoBool, AutoConnector, OptionalFields,
    )
    from connectors.abstract.leaf_connector import LeafConnector
    from streams.mixin.stream_builder_mixin import StreamBuilderMixin
    from streams.mixin.iterable_mixin import IterableStreamMixin
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg, dates as dt
    from ...interfaces import (
        Context, Connector, ConnectorInterface, IterableStreamInterface, ItemType, StreamType,
        AUTO, Auto, AutoName, AutoCount, AutoBool, AutoConnector, OptionalFields,
    )
    from ..abstract.leaf_connector import LeafConnector
    from ...streams.mixin.stream_builder_mixin import StreamBuilderMixin
    from ...streams.mixin.iterable_mixin import IterableStreamMixin

Stream = IterableStreamInterface
Native = Union[LeafConnector, Stream]

CHUNK_SIZE = 8192
LOGGING_LEVEL_INFO = 20
LOGGING_LEVEL_WARN = 30


class StreamFileMixin(StreamBuilderMixin, IterableStreamMixin, ABC):
    @abstractmethod
    def is_existing(self) -> bool:
        pass

    @abstractmethod
    def is_in_memory(self) -> bool:
        pass

    @staticmethod
    def get_default_item_type() -> ItemType:
        return ItemType.Any

    @classmethod
    def get_stream_type(cls):
        return StreamType.AnyStream

    @classmethod
    def get_stream_class(cls):
        return cls.get_stream_type().get_class()

    def _get_generated_stream_name(self) -> str:
        return arg.get_generated_name('{}:stream'.format(self.get_name()), include_random=True, include_datetime=False)

    @abstractmethod
    def from_stream(self, stream: Stream, verbose: bool = True) -> Native:
        pass

    def to_stream(
            self,
            data: Union[Iterable, Auto] = AUTO, name: AutoName = AUTO,
            stream_type: Union[StreamType, Auto] = AUTO, ex: OptionalFields = None,
            **kwargs
    ) -> Stream:
        stream_type = arg.delayed_acquire(stream_type, self.get_stream_type)
        name = arg.delayed_acquire(name, self._get_generated_stream_name)
        if not arg.is_defined(data):
            data = self.get_items()
        if isinstance(stream_type, str):
            stream_class = StreamType(stream_type).get_class()
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

    def stream(
            self, data: Union[Iterable, Auto] = AUTO,
            stream_type: Union[StreamType, Auto] = AUTO,
            ex: OptionalFields = None,
            **kwargs
    ) -> Stream:
        return self.to_stream(data, stream_type=stream_type, ex=ex, **kwargs)

    def map(self, function: Callable) -> Stream:
        return self.stream(
            map(function, self.get_items()),
        )

    def filter(self, function: Callable) -> Stream:
        return self.stream(
            filter(function, self.get_items()),
            count=None,
        )

    def add_stream(self, stream: Stream, **kwargs) -> Stream:
        stream = self.to_stream(**kwargs).add_stream(stream)
        return self._assume_stream(stream)

    def collect(self, skip_missing: bool = False, **kwargs) -> Stream:
        if self.is_existing():
            stream = self.to_stream(**kwargs)
            if hasattr(stream, 'collect'):
                stream = stream.collect()
            elif not skip_missing:
                raise TypeError('stream {} of type {} can not be collected'.format(stream, stream.get_stream_type()))
        elif skip_missing:
            stream = self.get_stream_class()([])
        else:
            raise FileNotFoundError('File {} not found'.format(self.get_name()))
        return self._assume_stream(stream)

    @staticmethod
    def _assume_stream(obj) -> Stream:
        return obj

    def get_children(self) -> dict:
        return self._data


class AbstractFile(LeafConnector, StreamFileMixin, ABC):
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

    def is_empty(self) -> bool:
        count = self.get_count(allow_slow_gzip=False) or 0
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


class TextFile(AbstractFile):
    def __init__(
            self,
            name: str,
            gzip: bool = False,
            encoding: str = 'utf8',
            end: str = '\n',
            expected_count: AutoCount = AUTO,
            folder: Connector = None,
            verbose: AutoBool = AUTO,
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
    def get_default_item_type() -> ItemType:
        return ItemType.Line

    @classmethod
    def get_stream_type(cls):
        return StreamType.LineStream

    def get_stream_data(self, verbose: AutoBool = AUTO, *args, **kwargs) -> Iterable:
        return self.get_items(verbose=verbose, *args, **kwargs)

    def is_gzip(self) -> bool:
        return self.gzip

    def open(self, mode: str = 'r', allow_reopen: bool = False) -> Native:
        if self.is_opened() is None or self.is_opened():
            if allow_reopen:
                self.close()
            else:
                raise AttributeError('File {} is already opened'.format(self.get_name()))
        path = self.get_path()
        if self.is_gzip():
            fileholder = gz.open(path, mode)
        else:
            params = dict()
            if self.encoding:
                params['encoding'] = self.encoding
            fileholder = open(path, mode, **params) if self.encoding else open(path, 'r')
        self.set_fileholder(fileholder)
        return self

    def get_prev_lines_count(self) -> Optional[int]:
        return self.count

    def get_slow_lines_count(self, verbose: AutoBool = AUTO) -> int:
        count = 0
        for _ in self.get_lines(message='Slow counting lines in {}...', allow_reopen=True, verbose=verbose):
            count += 1
        self.set_count(count)
        return count

    def get_fast_lines_count(self, verbose: AutoBool = AUTO) -> int:
        if self.is_gzip():
            raise ValueError('get_fast_lines_count() method is not available for gzip-files')
        verbose = arg.acquire(verbose, self.is_verbose())
        self.log('Counting lines in {}...'.format(self.get_name()), end='\r', verbose=verbose)
        count_n_symbol = sum(chunk.count('\n') for chunk in self.get_chunks())
        count_lines = count_n_symbol + 1
        self.set_count(count_lines)
        return count_lines

    def get_actual_lines_count(self, allow_reopen: bool = True, allow_slow_gzip: bool = True) -> Optional[int]:
        if self.is_opened():
            if allow_reopen:
                self.close()
            else:
                raise ValueError('File is already opened: {}'.format(self))
        self.open(allow_reopen=allow_reopen)
        if self.is_gzip():
            if allow_slow_gzip:
                count = self.get_slow_lines_count()
            else:
                count = None
        else:
            count = self.get_fast_lines_count()
        self.close()
        if count is not None:
            self.log('Detected {} lines in {}.'.format(count, self.get_name()), end='\r')
        return count

    def get_count(self, allow_reopen: bool = True, allow_slow_gzip: bool = True, force: bool = False) -> Optional[int]:
        must_recount = force or self.is_changed_by_another() or not arg.is_defined(self.get_prev_lines_count())
        if self.is_existing() and must_recount:
            count = self.get_actual_lines_count(allow_reopen=allow_reopen, allow_slow_gzip=allow_slow_gzip)
            self.set_count(count)
        else:
            count = self.get_prev_lines_count()
        if arg.is_defined(count):
            return count

    def set_count(self, count: int) -> Native:
        self.count = count
        return self

    def get_next_lines(self, count: Optional[int] = None, skip_first: bool = False, close: bool = False) -> Iterable:
        is_opened = self.is_opened()
        if is_opened is not None:
            assert is_opened, 'File must be opened for get_next_lines(), got is_opened={}'.format(is_opened)
        for n, row in enumerate(self.get_fileholder()):
            if skip_first and n == 0:
                continue
            if isinstance(row, bytes):
                row = row.decode(self.encoding) if self.encoding else row.decode()
            if self.end:
                row = row.rstrip(self.end)
            yield row
            if arg.is_defined(count):
                if count > 0 and (n + 1 == count):
                    break
        if close:
            self.close()

    def get_chunks(self, chunk_size=CHUNK_SIZE) -> Iterable:
        return iter(lambda: self.get_fileholder().read(chunk_size), '')

    def get_lines(
            self,
            count: Optional[int] = None,
            skip_first: bool = False, allow_reopen: bool = True,
            check: bool = True, verbose: AutoBool = AUTO,
            message: Union[str, Auto] = AUTO, step: AutoCount = AUTO,
    ) -> Iterable:
        if check and not self.is_gzip():
            assert not self.is_empty(), 'for get_lines() file must be non-empty: {}'.format(self)
        self.open(allow_reopen=allow_reopen)
        lines = self.get_next_lines(count=count, skip_first=skip_first, close=True)
        verbose = arg.acquire(verbose, self.is_verbose())
        if verbose or arg.is_defined(message):
            message = arg.acquire(message, 'Reading {}')
            if '{}' in message:
                message = message.format(self.get_name())
            logger = self.get_logger()
            assert hasattr(logger, 'progress'), '{} has no progress in {}'.format(self, logger)
            lines = self.get_logger().progress(lines, name=message, count=self.count, step=step)
        return lines

    def get_items(self, verbose: AutoBool = AUTO, step: AutoCount = AUTO) -> Iterable:
        verbose = arg.acquire(verbose, self.is_verbose())
        if isinstance(verbose, str):
            self.log(verbose, verbose=bool(verbose))
        elif (self.get_count() or 0) > 0:
            self.log('Expecting {} lines in file {}...'.format(self.get_count(), self.get_name()), verbose=verbose)
        return self.get_lines(verbose=verbose, step=step)

    def get_stream(self, to=AUTO, verbose: AutoBool = AUTO) -> Stream:
        to = arg.acquire(to, self.get_stream_type())
        return self.to_stream_class(
            stream_class=StreamType(to).get_class(),
            verbose=verbose,
        )

    def get_stream_kwargs(
            self, data: Union[Iterable, Auto] = AUTO, name: AutoName = AUTO,
            verbose: AutoBool = AUTO, step: AutoCount = AUTO,
            **kwargs
    ) -> dict:
        verbose = arg.acquire(verbose, self.is_verbose())
        data = arg.delayed_acquire(data, self.get_items, verbose=verbose, step=step)
        name = arg.delayed_acquire(name, self._get_generated_stream_name)
        result = dict(
            data=data, name=name, source=self,
            count=self.get_count(), context=self.get_context(),
        )
        result.update(kwargs)
        return result

    def to_stream_class(self, stream_class, **kwargs) -> Stream:
        return stream_class(
            **self.get_stream_kwargs(**kwargs)
        )

    def to_line_stream(self, step: AutoCount = AUTO, verbose: AutoBool = AUTO, **kwargs) -> Stream:
        data = self.get_lines(step=step, verbose=verbose)
        stream_kwargs = self.get_stream_kwargs(data=data, step=step, verbose=verbose, **kwargs)
        return StreamType.LineStream.stream(**stream_kwargs)

    def to_any_stream(self, **kwargs) -> Stream:
        return StreamType.AnyStream.stream(
            **self.get_stream_kwargs(**kwargs)
        )

    def write_lines(self, lines: Iterable, verbose: AutoBool = AUTO) -> Native:
        verbose = arg.acquire(verbose, self.is_verbose())
        self.open('w', allow_reopen=True)
        n = 0
        for n, i in enumerate(lines):
            if n > 0:
                self.get_fileholder().write(self.end.encode(self.encoding) if self.gzip else self.end)
            self.get_fileholder().write(str(i).encode(self.encoding) if self.gzip else str(i))
        self.close()
        count = n + 1
        self.set_count(count)
        self.log('Done. {} rows has written into {}'.format(count, self.get_name()), verbose=verbose)
        return self

    def write_stream(self, stream: Stream, verbose: AutoBool = AUTO) -> Native:
        if hasattr(stream, 'to_line_stream'):
            return self.write_lines(
                stream.to_line_stream().get_items(),
                verbose=verbose,
            )
        else:
            raise TypeError('stream-argument must be a Stream (got {})'.format(stream))

    def from_stream(self, stream: Stream, verbose: bool = True) -> Native:
        assert stream.get_stream_type() == StreamType.LineStream
        return self.write_lines(stream.get_iter(), verbose=verbose)

    def skip(self, count: int = 1) -> Stream:
        stream = self.to_line_stream().skip(count)
        return self._assume_stream(stream)

    def take(self, count: int = 10) -> Stream:
        stream = self.to_line_stream().take(count)
        return self._assume_stream(stream)

    def apply_to_data(self, function: Callable, *args, dynamic: bool = False, **kwargs) -> Stream:
        stream = self.stream(  # can be file
            function(self.get_items(), *args, **kwargs),
        ).set_meta(
            **self.get_static_meta() if dynamic else self.get_meta()
        )
        return self._assume_stream(stream)

    def _get_demo_example(self, count: int = 10, filters: Optional[list] = None) -> Optional[Iterable]:
        if self.is_existing():
            stream_sample = self.filter(*filters or []) if filters else self
            stream_sample = stream_sample.take(count)
            return stream_sample.get_items()

    def get_useful_props(self) -> dict:
        if self.is_existing():
            return dict(
                folder=self.get_folder_path(),
                is_actual=self.is_actual(),
                is_opened=self.is_opened(),
                is_empty=self.is_empty(),
                count=self.get_count(),
                path=self.get_path(),
            )
        else:
            return dict(
                is_existing=self.is_existing(),
                folder=self.get_folder_path(),
                path=self.get_path(),
            )

    def get_str_meta(self, useful_only: bool = False) -> str:
        if useful_only:
            args_str = ["'{}'".format(self.get_name())]
            kwargs_str = ["{}={}".format(k, v) for k, v in self.get_useful_props().items()]
            return ', '.join(args_str + kwargs_str)
        else:
            return super().get_str_meta()

    def get_str_headers(self) -> Iterable:
        yield '{}({})'.format(self.__class__.__name__, self.get_str_meta(useful_only=True))

    def describe(self, count: Optional[int] = 10, filters: Optional[list] = None, show_header: bool = True):
        if show_header:
            for line in self.get_str_headers():
                self.log(line, end='\n')
        if self.is_existing():
            self.actualize()
            self.log('{} File has {} lines'.format(self.get_datetime_str(), self.get_count()))
            self.log('')
            for line in self._get_demo_example(count=count, filters=filters):
                self.log(line)
        return self

    def to_stream(
            self,
            data: Union[Iterable, Auto] = AUTO, name: AutoName = AUTO,
            stream_type: Union[StreamType, Auto] = AUTO, ex: OptionalFields = None,
            **kwargs
    ) -> Stream:
        stream_type = arg.delayed_acquire(stream_type, self.get_stream_type)
        name = arg.delayed_acquire(name, self._get_generated_stream_name)
        if not arg.is_defined(data):
            data = self.get_items()
        if isinstance(stream_type, str):
            stream_class = StreamType(stream_type).get_class()
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


class JsonFile(TextFile):
    def __init__(
            self,
            name: str,
            encoding: str = 'utf8',
            gzip: bool = False,
            expected_count: AutoCount = AUTO,
            struct=AUTO,
            default_value: Any = None,
            folder: Connector = None,
            verbose: AutoBool = AUTO,
    ):
        super().__init__(
            name=name,
            encoding=encoding,
            gzip=gzip,
            expected_count=expected_count,
            folder=folder,
            verbose=verbose,
        )
        self._struct = struct
        self._default_value = default_value

    @staticmethod
    def get_default_file_extension() -> str:
        return 'json'

    @staticmethod
    def get_default_item_type() -> ItemType:
        return ItemType.Any

    @classmethod
    def get_stream_type(cls):
        return StreamType.AnyStream

    def get_items(self, verbose: AutoBool = AUTO, step: AutoCount = AUTO) -> Iterable:
        stream = self.to_line_stream(verbose=verbose)
        if hasattr(stream, 'parse_json'):
            return stream.parse_json(default_value=self._default_value).get_items()
        else:
            raise NotImplementedError

    def to_record_stream(self, verbose: AutoBool = AUTO, **kwargs) -> Stream:
        return StreamType.RecordStream.stream(
            self.get_items(verbose=verbose),
            count=self.get_count(),
            source=self,
            context=self.get_context(),
            **kwargs
        )

    def write_stream(self, stream: Stream, verbose: AutoBool = AUTO) -> Native:
        if hasattr(stream, 'to_json'):
            lines = stream.to_json().get_items()
        else:
            raise TypeError('for write to json-file stream must support .to_json() method (got {})'.format(stream))
        return self.write_lines(lines, verbose=verbose)
