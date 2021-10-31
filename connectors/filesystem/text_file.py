from inspect import isclass
from typing import Iterable, Optional, Union, Callable, Any
import gzip as gz

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from utils.decorators import deprecated, deprecated_with_alternative
    from interfaces import (
        Connector, IterableStreamInterface, StreamType, ItemType, FileType,
        AUTO, AutoCount, AutoBool, Auto, AutoName, OptionalFields,
    )
    from connectors.filesystem.abstract_file import CHUNK_SIZE, AbstractFile
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...utils.decorators import deprecated, deprecated_with_alternative
    from ...interfaces import (
        Connector, IterableStreamInterface, StreamType, ItemType, FileType,
        AUTO, AutoCount, AutoBool, Auto, AutoName, OptionalFields,
    )
    from .abstract_file import CHUNK_SIZE, AbstractFile

Native = AbstractFile
Stream = IterableStreamInterface


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
    def get_stream_type(cls) -> StreamType:
        return StreamType.LineStream

    def get_content_type(self) -> FileType:
        return FileType.TextFile

    def get_stream_data(self, verbose: AutoBool = AUTO, *args, **kwargs) -> Iterable:
        return self.get_items(verbose=verbose, *args, **kwargs)

    def get_expected_count(self) -> AutoCount:
        return self.count

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
            if not arg.is_defined(count):
                count = self.get_count(allow_slow_gzip=False)
            lines = self.get_logger().progress(lines, name=message, count=count, step=step)
        return lines

    def get_items(
            self,
            item_type: Union[ItemType, Auto] = AUTO,
            verbose: AutoBool = AUTO,
            step: AutoCount = AUTO,
    ) -> Iterable:
        item_type = arg.delayed_acquire(item_type, self.get_default_item_type)
        assert item_type == ItemType.Line
        verbose = arg.acquire(verbose, self.is_verbose())
        if isinstance(verbose, str):
            self.log(verbose, verbose=bool(verbose))
        elif (self.get_count() or 0) > 0:
            self.log('Expecting {} lines in file {}...'.format(self.get_count(), self.get_name()), verbose=verbose)
        return self.get_lines(verbose=verbose, step=step)

    @deprecated_with_alternative('to_stream()')
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
        expected_count = self.get_count(allow_slow_gzip=False)
        result = dict(
            data=data, name=name, source=self,
            count=expected_count, context=self.get_context(),
        )
        result.update(kwargs)
        return result

    @deprecated
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
        if hasattr(stream, 'get_item_type'):  # isinstance(stream, Regular)
            item_type = stream.get_item_type()
            if item_type in (ItemType.Row, ItemType.Record, ItemType.Line):
                item_type_str = item_type.get_value()
                method_name = 'write_{}s'.format(item_type_str)
                method = self.__getattribute__(method_name)
                return method(stream.get_items(), verbose=verbose)
            else:
                message = '{}.write_stream() supports LineStream, RowStream, RecordStream only (got {})'
                raise TypeError(message.format(self.__class__.__name__, stream.__class__.__name__))
        elif hasattr(stream, 'to_line_stream'):
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

    def get_content_type(self) -> FileType:
        return FileType.JsonFile

    def get_items(
            self,
            item_type: Union[ItemType, Auto] = AUTO,
            verbose: AutoBool = AUTO,
            step: AutoCount = AUTO,
    ) -> Iterable:
        item_type = arg.delayed_acquire(item_type, self.get_default_item_type)
        if item_type == ItemType.Line:
            return super().get_items(item_type=item_type, verbose=verbose, step=step)
        else:
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
