from typing import Optional, Iterable, Generator, Union, Any
import os
import gzip as gz

try:  # Assume we're a submodule in a package.
    from interfaces import (
        Context, Connector, ConnectorInterface, ContentFormatInterface, StructInterface,
        IterableStreamInterface, RegularStreamInterface,
        ContentType, ConnType, ItemType, StreamType,
        AUTO, Auto, AutoCount, AutoBool, AutoName, OptionalFields, UniKey, Array, ARRAY_TYPES,
    )
    from functions.primary.text import is_formatter
    from content.format.format_classes import (
        AbstractFormat, ParsedFormat, LeanFormat,
        TextFormat, ColumnarFormat, FlatStructFormat,
    )
    from connectors.abstract.leaf_connector import LeafConnector
    from connectors.mixin.connector_format_mixin import ConnectorFormatMixin
    from connectors.mixin.actualize_mixin import ActualizeMixin
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        Context, Connector, ConnectorInterface, ContentFormatInterface, StructInterface,
        IterableStreamInterface, RegularStreamInterface,
        ContentType, ConnType, ItemType, StreamType,
        AUTO, Auto, AutoCount, AutoBool, AutoName, OptionalFields, UniKey, Array, ARRAY_TYPES,
    )
    from ...functions.primary.text import is_formatter
    from ...content.format.format_classes import (
        AbstractFormat, ParsedFormat, LeanFormat,
        TextFormat, ColumnarFormat, FlatStructFormat,
    )
    from ..abstract.leaf_connector import LeafConnector
    from ..mixin.connector_format_mixin import ConnectorFormatMixin
    from ..mixin.actualize_mixin import ActualizeMixin

Stream = IterableStreamInterface
Struct = Optional[StructInterface]
Native = Union[LeafConnector, Stream]

CHUNK_SIZE = 8192
LOGGING_LEVEL_INFO = 20
LOGGING_LEVEL_WARN = 30


class LocalFile(LeafConnector, ActualizeMixin):
    _default_folder: Connector = None

    def __init__(
            self,
            name: str,
            content_format: Union[ContentFormatInterface, Auto] = AUTO,
            struct: Union[Struct, Auto, None] = AUTO,
            folder: Connector = None,
            context: Context = AUTO,
            first_line_is_title: AutoBool = AUTO,
            expected_count: AutoCount = AUTO,
            caption: Optional[str] = None,
            verbose: AutoBool = AUTO,
            **kwargs
    ):
        parent = kwargs.pop('parent', None)
        if folder:
            message = 'only LocalFolder supported for *File instances (got {})'.format(type(folder))
            assert isinstance(folder, ConnectorInterface) or folder.is_folder(), message
            assert folder == parent or not Auto.is_defined(parent)
        elif Auto.is_defined(parent):
            folder = parent
        elif Auto.is_defined(context):
            folder = context.get_job_folder()
        else:
            folder = self.get_default_folder()
        self._fileholder = None
        super().__init__(
            name=name,
            content_format=content_format, struct=struct,
            first_line_is_title=first_line_is_title,
            expected_count=expected_count,
            caption=caption,
            parent=folder, context=context, verbose=verbose,
            **kwargs,
        )

    def get_folder(self) -> Union[Connector, Any]:
        return self.get_parent()

    def get_children(self) -> dict:
        return self._data

    def get_slow_lines_count(self, verbose: AutoBool = AUTO) -> int:
        count = 0
        for _ in self.get_lines(message='Slow counting lines in {}...', allow_reopen=True, verbose=verbose):
            count += 1
        self.set_count(count)
        return count

    def get_fast_lines_count(self, ending: Union[str, Auto] = AUTO, verbose: AutoBool = AUTO) -> int:
        if self.is_gzip():
            raise ValueError('get_fast_lines_count() method is not available for gzip-files')
        if not Auto.is_defined(ending):
            if hasattr(self, 'get_content_format'):
                ending = self.get_content_format().get_ending()
            else:
                ending = '\n'
        verbose = Auto.acquire(verbose, self.is_verbose())
        self.log('Counting lines in {}...'.format(self.get_name()), end='\r', verbose=verbose)
        count_n_symbol = sum(chunk.count(ending) for chunk in self.get_chunks())
        count_lines = count_n_symbol + 1
        self.set_count(count_lines)
        return count_lines

    def get_actual_lines_count(self, allow_reopen: bool = True, allow_slow_mode: bool = True) -> Optional[int]:
        if self.is_opened():
            if allow_reopen:
                self.close()
            else:
                raise ValueError('File is already opened: {}'.format(self))
        self.open(allow_reopen=allow_reopen)
        if self.is_gzip():
            if allow_slow_mode:
                count = self.get_slow_lines_count()
            else:
                count = None
        else:
            count = self.get_fast_lines_count()
        self.close()
        if count is not None:
            self.log('Detected {} lines in {}.'.format(count, self.get_name()), end='\r')
        return count

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

    def get_default_file_extension(self) -> str:
        """Returns expected (recommended) file extension for files with this content format."""
        return self.get_content_type().get_default_file_extension()

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
            if '*' in folder_path:  # isinstance(self.get_parent(), LocalMask):
                folder_path = folder_path.replace('*', '{}')
            if is_formatter(folder_path):  # isinstance(self.get_parent(), LocalMask):
                return folder_path.format(self.get_name())
            elif folder_path.endswith(self.get_path_delimiter()):
                return folder_path + self.get_name()
            elif folder_path:
                return f'{folder_path}{self.get_path_delimiter()}{self.get_name()}'
            else:
                return self.get_name()

    def get_full_path(self) -> str:
        folder = self.get_folder()
        if not folder:
            folder = self.get_storage()  # job_folder
        folder_path = folder.get_full_path()
        if '*' in folder_path:  # isinstance(self.get_parent(), LocalMask):
            folder_path = folder_path.replace('*', '{}')
        if is_formatter(folder_path):  # isinstance(self.get_parent(), LocalMask):
            return folder_path.format(self.get_name())
        elif folder_path.endswith(self.get_path_delimiter()):
            return folder_path + self.get_name()
        elif folder_path:
            return f'{folder_path}{self.get_path_delimiter()}{self.get_name()}'
        else:
            return self.get_name()

    def get_list_path(self) -> Iterable:
        return self.get_path().split(self.get_path_delimiter())

    def get_folder_path(self) -> str:
        return self.get_path_delimiter().join(self.get_list_path()[:-1])

    def is_inside_folder(self, folder: Union[str, Connector, Auto] = AUTO) -> bool:
        folder_obj = Auto.acquire(folder, self.get_folder())
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
            closed_count = 1
        else:
            closed_count = 0
        if self.is_gzip():
            self.set_fileholder(None)
        return closed_count

    def open(self, mode: str = 'r', allow_reopen: bool = False) -> Native:
        is_opened = self.is_opened()
        if is_opened or (is_opened is None):
            if allow_reopen:
                self.close()
            else:
                raise AttributeError('File {} is already opened'.format(self.get_name()))
        path = self.get_path()
        if self.is_gzip():
            fileholder = gz.open(path, mode)
        else:
            params = dict()
            encoding = self.get_encoding()
            if encoding:
                params['encoding'] = encoding
            fileholder = open(path, mode, **params)
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

    def is_empty(self) -> bool:
        count = self.get_count(allow_slow_mode=False) or 0
        return count <= 0

    def has_data(self) -> bool:
        if self.is_existing():
            return not self.is_empty()
        return False

    @staticmethod
    def is_file() -> bool:
        return True

    def is_text_file(self) -> bool:
        return self.get_content_format().is_text()

    def get_modification_timestamp(self, reset: bool = True) -> Optional[float]:
        if self.is_existing():
            timestamp = os.path.getmtime(self.get_path())
            if reset or not self.get_prev_modification_timestamp():
                self.set_prev_modification_timestamp(timestamp)
            return timestamp

    def get_first_line(self, close: bool = True, skip_missing: bool = False, verbose: bool = False) -> str:
        if not skip_missing:
            content_format = self.get_content_format()
            assert isinstance(content_format, ParsedFormat), f'For get first line content must be parsed: {self}'
            assert content_format.get_defined().is_text(), f'For parse content format must be text: {self}'
            assert not self.is_empty(), f'For get line file/object must be non-empty: {self}'
        return super().get_first_line(close=close, skip_missing=skip_missing, verbose=verbose)

    def get_next_lines(self, count: Optional[int] = None, skip_first: bool = False, close: bool = False) -> Iterable:
        is_opened = self.is_opened()
        if is_opened is not None:
            assert is_opened, 'For LocalFile.get_next_lines() file must be opened: {}'.format(self)
        encoding = self.get_encoding()
        ending = self.get_ending()
        iter_lines = self.get_fileholder()
        for n, line in enumerate(iter_lines):
            if skip_first and n == 0:
                continue
            if isinstance(line, bytes):
                line = line.decode(encoding) if encoding else line.decode()
            if ending:
                line = line.rstrip(ending)
            yield line
            if Auto.is_defined(count):
                if count > 0 and (n + 1 == count):
                    break
        if close:
            self.close()

    def get_lines(
            self,
            count: Optional[int] = None,
            skip_first: bool = False,
            skip_missing: bool = True,
            allow_reopen: bool = True,
            verbose: AutoBool = AUTO,
            message: AutoName = AUTO,
            step: AutoCount = AUTO,
    ) -> Generator:
        if skip_missing and not self.is_gzip():
            assert not self.is_empty(), 'for get_lines() file must be non-empty: {}'.format(self)
        self.open(allow_reopen=allow_reopen)
        lines = self.get_next_lines(count=count, skip_first=skip_first, close=True)
        verbose = Auto.acquire(verbose, self.is_verbose())
        if verbose or Auto.is_defined(message):
            if not Auto.is_defined(message):
                message = 'Reading {}'
            if '{}' in message:
                message = message.format(self.get_name())
            logger = self.get_logger()
            assert hasattr(logger, 'progress'), '{} has no progress in {}'.format(self, logger)
            if not count:
                count = self.get_count(allow_slow_mode=False)
            lines = self.get_logger().progress(lines, name=message, count=count, step=step)
        return lines

    def get_chunks(self, chunk_size=CHUNK_SIZE) -> Iterable:
        return iter(lambda: self.get_fileholder().read(chunk_size), '')

    def write_lines(self, lines: Iterable, verbose: AutoBool = AUTO) -> Native:
        verbose = Auto.acquire(verbose, self.is_verbose())
        ending = self.get_ending().encode(self.get_encoding()) if self.is_gzip() else self.get_ending()
        self.open('w', allow_reopen=True)
        n = 0
        for n, i in enumerate(lines):
            if n > 0:
                self.get_fileholder().write(ending)
            line = str(i).encode(self.get_encoding()) if self.is_gzip() else str(i)
            self.get_fileholder().write(line)
        self.close()
        count = n + 1
        self.set_count(count)
        self.log('Done. {} rows has written into {}'.format(count, self.get_name()), verbose=verbose)
        return self

    def write_items(
            self,
            items: Iterable,
            item_type: Union[ItemType, Auto] = AUTO,
            add_title_row: AutoBool = AUTO,
            verbose: AutoBool = AUTO,
    ) -> Native:
        item_type = Auto.delayed_acquire(item_type, self.get_default_item_type)
        content_format = self.get_content_format()
        assert isinstance(content_format, ParsedFormat)
        lines = content_format.get_lines(items, item_type=item_type, add_title_row=add_title_row)
        return self.write_lines(lines, verbose=verbose)

    def write_stream(
            self,
            stream: IterableStreamInterface,
            add_title_row: AutoBool = AUTO,
            verbose: AutoBool = AUTO,
    ) -> Native:
        if hasattr(stream, 'get_item_type'):
            item_type = stream.get_item_type()
        else:
            item_type = ItemType.detect(stream.get_one_item())
        return self.write_items(stream.get_items(), item_type=item_type, add_title_row=add_title_row, verbose=verbose)

    def to_stream(
            self,
            data: Union[Iterable, Auto] = AUTO,
            name: AutoName = AUTO,
            stream_type: Union[StreamType, Auto] = AUTO,
            ex: OptionalFields = None,
            step: AutoCount = AUTO,
            **kwargs
    ) -> Stream:
        if Auto.is_defined(data):
            kwargs['data'] = data
        stream_type = Auto.delayed_acquire(stream_type, self.get_stream_type)
        assert not ex, 'ex-argument for LocalFile.to_stream() not supported (got {})'.format(ex)
        return self.to_stream_type(stream_type=stream_type, step=step, **kwargs)

    @classmethod
    def get_default_folder(cls) -> Connector:
        return cls._default_folder

    @classmethod
    def set_default_folder(cls, folder: ConnectorInterface) -> None:
        cls._default_folder = folder

    def _get_field_getter(self, field: UniKey, item_type: Union[ItemType, Auto] = AUTO, default=None):
        if self.get_struct():
            if isinstance(field, ARRAY_TYPES):
                fields_positions = self.get_fields_positions(field)
                return lambda i: tuple([i[p] for p in fields_positions])
            else:
                field_position = self.get_field_position(field)
                return lambda i: i[field_position]
        else:
            return super()._get_field_getter(field, item_type=item_type, default=default)


ConnType.add_classes(LocalFile)
