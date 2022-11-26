from abc import ABC, abstractmethod
import inspect
from typing import Optional, Callable, Iterable, Sequence, Tuple, Union, Any
import gc

try:  # Assume we're a submodule in a package.
    from interfaces import (
        StreamInterface, LoggerInterface, LeafConnectorInterface,
        Stream, ExtLogger, Context, Connector, LeafConnector,
        StreamType, LoggingLevel,
        AUTO, Auto, AutoName, OptionalFields, Array, Class, Message,
    )
    from base.functions.arguments import get_generated_name
    from base.constants.chars import CROP_SUFFIX, DEFAULT_LINE_LEN, EMPTY
    from base.abstract.contextual_data import ContextualDataWrapper
    from utils.decorators import deprecated_with_alternative
    from loggers.fallback_logger import FallbackLogger
    from streams.interfaces.abstract_stream_interface import StreamInterface, DEFAULT_EXAMPLE_COUNT
    from streams.stream_builder import StreamBuilder
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        StreamInterface, LoggerInterface, LeafConnectorInterface,
        Stream, ExtLogger, Context, Connector, LeafConnector,
        StreamType, LoggingLevel,
        AUTO, Auto, AutoName, OptionalFields, Array, Class, Message,
    )
    from ...base.functions.arguments import get_generated_name
    from ...base.constants.chars import CROP_SUFFIX, DEFAULT_LINE_LEN, EMPTY
    from ...base.abstract.contextual_data import ContextualDataWrapper
    from ...utils.decorators import deprecated_with_alternative
    from ...loggers.fallback_logger import FallbackLogger
    from ..interfaces.abstract_stream_interface import StreamInterface, DEFAULT_EXAMPLE_COUNT
    from ..stream_builder import StreamBuilder

Native = StreamInterface

DATA_MEMBERS = '_data',


class AbstractStream(ContextualDataWrapper, StreamInterface, ABC):
    def __init__(
            self,
            data: Any,
            name: AutoName = AUTO,
            caption: str = EMPTY,
            source: Connector = None,
            context: Context = None,
            check: bool = False,
    ):
        if source:
            name = Auto.acquire(name, source.get_name())
        else:
            name = Auto.acquire(name, get_generated_name())
        if source and not context:
            context = source.get_context()
        if not context:
            context = StreamBuilder.get_context()
        super().__init__(name=name, caption=caption, data=data, source=source, context=context, check=check)

    def set_name(self, name: str, register: bool = True, inplace: bool = False) -> Optional[Native]:
        if register:
            old_name = self.get_name()
            self.get_context().rename_stream(old_name, name)
        return super().set_name(name, inplace=inplace)

    def get_type(self) -> StreamType:
        return self.get_stream_type()

    @classmethod
    def _get_data_member_names(cls) -> tuple:
        return DATA_MEMBERS

    @abstractmethod
    def get_count(self):
        raise NotImplemented

    @abstractmethod
    def get_items(self) -> Iterable:
        pass

    def get_links(self) -> Iterable:
        source = self.get_source()
        if source:
            yield source

    def set_meta(self, inplace: bool = False, **meta) -> Native:
        stream = super().set_meta(**meta, inplace=inplace)
        return self._assume_native(stream)

    def update_meta(self, **meta) -> Native:
        stream = super().update_meta(**meta)
        return self._assume_native(stream)

    def fill_meta(self, check=True, **meta) -> Native:
        stream = super().fill_meta(check=check, **meta)
        return self._assume_native(stream)

    def get_property(self, name, *args, **kwargs) -> Any:
        if callable(name):
            value = name(self)
        elif isinstance(name, str):
            meta = self.get_meta()
            if name in meta:
                value = meta.get(name)
            else:
                try:
                    value = self.__getattribute__(name)(*args, **kwargs)
                except AttributeError:
                    value = None
        else:
            raise TypeError('property name must be function, meta-field or attribute name')
        return value

    def _get_calc(self, function: Callable, *args, **kwargs) -> Any:
        return function(self.get_data(), *args, **kwargs)

    def apply_to_data(self, function: Callable, dynamic=False, *args, **kwargs) -> Native:
        items = self._get_calc(function, *args, **kwargs)
        excluded_fields = self._get_dynamic_meta_fields() if dynamic else None
        return self.stream(items, ex=excluded_fields)  # can be a file

    def apply_to_stream(self, function: Callable, *args, **kwargs) -> Union[Native, Any]:
        return function(self, *args, **kwargs)

    def apply(self, function: Callable, *args, to_stream: bool = False, **kwargs) -> Union[Native, Any]:
        if to_stream:
            return self.apply_to_stream(function, *args, **kwargs)
        else:
            return self.apply_to_data(function, *args, **kwargs)

    @classmethod
    def get_stream_type(cls) -> StreamType:
        stream_type = StreamType.detect(cls)
        assert isinstance(stream_type, StreamType)
        return stream_type

    @classmethod
    def get_class(cls, other: Stream = None) -> Class:
        if not Auto.is_auto(other):
            return StreamBuilder.get_default_stream_class()
        elif isinstance(other, (StreamType, str)):
            return StreamType(other).get_class()
        elif inspect.isclass(other):
            return other
        else:
            raise TypeError('"other" parameter must be class or StreamType (got {})'.format(type(other)))

    def stream(self, data: Any, ex: OptionalFields = None, **kwargs) -> Native:
        meta = self.get_meta(ex=ex)
        meta.update(kwargs)
        stream = self.__class__(data, **meta)
        assert isinstance(stream, Native)
        return stream

    def to_stream(self, *args, **kwargs) -> Native:
        msg = 'args and kwargs for to_stream() supported only for stream subclasses'
        assert not args, msg
        assert not kwargs, msg
        return self

    def write_to(self, connector: LeafConnector, verbose: bool = True, return_stream: bool = True) -> Optional[Native]:
        msg = 'connector-argument must be an instance of LeafConnector or have write_stream() method'
        assert isinstance(connector, LeafConnectorInterface) or hasattr(connector, 'write_stream'), msg
        connector.write_stream(self, verbose=verbose)
        if return_stream:
            return connector.to_stream(verbose=verbose).update_meta(**self.get_meta())

    def get_logger(self, skip_missing: bool = True) -> LoggerInterface:
        context = self.get_context()
        if context:
            logger = context.get_logger(create_if_not_yet=skip_missing)
        else:
            logger = None
        if skip_missing and not logger:
            return FallbackLogger()
        return logger

    def log(
            self,
            msg: Message,
            level: LoggingLevel = AUTO,
            end: str = AUTO,
            truncate: bool = True,
            # force: bool = False,  # ?
            force: bool = True,  # ?
            verbose: bool = True,
    ) -> Native:
        logger = self.get_logger(skip_missing=force)
        if isinstance(logger, ExtLogger):
            logger.log(msg=msg, level=level, end=end, truncate=truncate, verbose=verbose)
        elif logger:
            logger.log(msg=msg, level=level)
        return self

    def forget(self) -> None:
        if hasattr(self, 'close'):
            self.close()
        context = self.get_context()
        if context:
            context.forget_child(self)
        gc.collect()

    def get_description(self) -> str:
        return 'with meta {}'.format(self.get_meta())

    def get_one_line_repr(
            self,
            str_meta: Union[str, Auto, None] = AUTO,
            max_len: int = DEFAULT_LINE_LEN,
            crop: str = CROP_SUFFIX,
    ) -> str:
        str_meta = Auto.delayed_acquire(str_meta, self.get_str_meta)
        return '{}({}, {})'.format(self.__class__.__name__, self.get_name(), str_meta)

    def __repr__(self):
        return self.get_brief_repr()

    def __str__(self):
        return self.get_one_line_repr()

    def _get_demo_example(
            self,
            count: int = DEFAULT_EXAMPLE_COUNT,
            filters: Optional[Array] = None,
            columns: Optional[Array] = None,
            example: Optional[Stream] = None,
    ) -> Native:
        if hasattr(self, 'is_in_memory'):  # isinstance(self, IterableStream)
            is_in_memory = self.is_in_memory()
        else:
            is_in_memory = True  # ?
        if Auto.is_defined(example):
            stream = example
        elif is_in_memory:
            stream = self
        else:  # data is iterator
            stream = self.copy()
        if Auto.is_defined(filters):
            stream = stream.filter(*filters)
        if Auto.is_defined(count):
            stream = stream.take(count)
        if Auto.is_defined(columns) and hasattr(stream, 'select'):
            stream = stream.select(columns)
        return stream.collect()

    def _get_demo_records_and_columns(
            self,
            count: int = DEFAULT_EXAMPLE_COUNT,
            columns: Optional[Array] = None,
            filters: Optional[Array] = None,
            example: Union[Stream, None] = None,
    ) -> Tuple[Sequence, Sequence]:
        example = self._get_demo_example(count=count, columns=columns, filters=filters, example=example)
        if hasattr(example, 'get_columns') and hasattr(example, 'get_records'):  # RegularStream, SqlStream
            records = example.get_records()  # ConvertMixin.get_records(), SqlStream.get_records()
            columns = example.get_columns()  # StructMixin.get_columns(), RegularStream.get_columns()
        else:
            item_field = 'item'
            records = [{item_field: i} for i in example]
            columns = [item_field]
        return records, columns

    def show(self, *args, **kwargs):
        display = self.get_display()
        display.display_paragraph(self.get_name(), level=2)
        records, columns = self._get_demo_records_and_columns(*args, **kwargs)
        display.display_sheet(records, columns=columns)
        return self

    @staticmethod
    def _assume_native(stream) -> Native:
        return stream
