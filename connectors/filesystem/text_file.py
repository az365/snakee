from typing import Iterable, Optional, Any

try:  # Assume we're a submodule in a package.
    from utils import arguments as arg
    from utils.decorators import deprecated, deprecated_with_alternative
    from interfaces import (
        Connector, Context, IterableStreamInterface,
        StreamType, ItemType, FileType, ContentType,
        AUTO, AutoCount, AutoBool, Auto, AutoName, OptionalFields, Array,
    )
    from connectors.filesystem.local_file import LocalFile, Struct, AbstractFormat
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...utils.decorators import deprecated, deprecated_with_alternative
    from ...interfaces import (
        Connector, Context, IterableStreamInterface,
        StreamType, ItemType, FileType, ContentType,
        AUTO, AutoCount, AutoBool, Auto, AutoName, OptionalFields, Array,
    )
    from .local_file import LocalFile, Struct, AbstractFormat

Native = LocalFile
Stream = IterableStreamInterface


class TextFile(LocalFile):
    # @deprecated_with_alternative('LocalFile')
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
        content_class = ContentType.TextFile.get_class()
        content_format = content_class(ending=end, encoding=encoding, compress='gzip' if gzip else None)
        super().__init__(
            name=name, content_format=content_format,
            folder=folder, expected_count=expected_count, verbose=verbose,
        )

    def get_default_file_extension(self) -> str:
        return 'txt'

    @staticmethod
    def get_default_item_type() -> ItemType:
        return ItemType.Line

    @classmethod
    def get_stream_type(cls) -> StreamType:
        return StreamType.LineStream

    def get_content_type(self) -> ContentType:
        return ContentType.TextFile

    def get_stream_data(self, verbose: AutoBool = AUTO, *args, **kwargs) -> Iterable:
        return self.get_items(verbose=verbose, *args, **kwargs)

    @deprecated_with_alternative('to_stream()')
    def get_stream(self, to=AUTO, verbose: AutoBool = AUTO) -> Stream:
        to = arg.acquire(to, self.get_stream_type())
        return self.to_stream_class(
            stream_class=StreamType(to).get_class(),
            verbose=verbose,
        )

    @deprecated
    def to_stream_class(self, stream_class, **kwargs) -> Stream:
        return stream_class(
            **self.get_stream_kwargs(**kwargs)
        )

    def _get_demo_example(
            self,
            count: int = 10,
            columns: Optional[Array] = None,
            filters: Optional[Array] = None,
            filter_kwargs: Optional[dict] = None,
    ) -> Optional[Iterable]:
        if self.is_existing():
            stream_example = self
            if filters or filter_kwargs:
                stream_example = stream_example.filter(*filters or [], **filter_kwargs or {})
            if columns:
                stream_example = stream_example.select(*columns)
            if count:
                stream_example = stream_example.take(count)
            return stream_example.get_items()

    def get_useful_props(self) -> dict:
        if self.is_existing():
            return dict(
                folder=self.get_folder_path(),
                is_actual=self.is_actual(),
                is_opened=self.is_opened(),
                is_empty=self.is_empty(),
                count=self.get_count(allow_slow_gzip=False),
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

    def describe(
            self,
            count: Optional[int] = 10,
            show_header: bool = True,
            struct_as_dataframe: bool = False,
            safe_filter: bool = True,
            actualize: bool = AUTO,
            columns: Optional[Array] = None,
            *filter_args, **filter_kwargs
    ):
        if show_header:
            for line in self.get_str_headers():
                self.log(line, end='\n')
        if self.is_existing():
            force = arg.delayed_acquire(actualize, self.is_outdated)
            self.actualize(if_outdated=not force)
            self.log('{} File has {} lines'.format(self.get_datetime_str(), self.get_count(allow_slow_gzip=False)))
            self.log('')
            for line in self._get_demo_example(
                    count=count, columns=columns,
                    filters=filter_args, filter_kwargs=filter_kwargs,
            ):
                self.log(line)
        return self


class JsonFile(LocalFile):
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
        self._default_value = default_value
        content_class = ContentType.JsonFile.get_class()
        content_format = content_class(encoding=encoding, compress='gzip' if gzip else None)
        super().__init__(
            name=name, content_format=content_format, struct=struct,
            folder=folder, expected_count=expected_count, verbose=verbose,
        )

    def get_default_file_extension(self) -> str:
        return 'json'

    @staticmethod
    def get_default_item_type() -> ItemType:
        return ItemType.Any

    @classmethod
    def get_stream_type(cls):
        return StreamType.AnyStream

    def get_content_type(self) -> ContentType:
        return ContentType.JsonFile
