from abc import ABC, abstractmethod
import inspect
from typing import Optional, Callable, Iterable, Union, Any
import gc

try:  # Assume we're a submodule in a package.
    from interfaces import (
        StreamInterface, LoggerInterface, LeafConnectorInterface, LeafConnector, Connector, Context, ExtLogger,
        StreamType, LoggingLevel, Message, Name, OptionalFields, Array, Class,
    )
    from base.constants.chars import EMPTY, CROP_SUFFIX
    from base.constants.text import DEFAULT_LINE_LEN
    from base.functions.arguments import get_generated_name, get_cropped_text
    from base.abstract.contextual_data import ContextualDataWrapper
    from utils.decorators import deprecated_with_alternative
    from loggers.fallback_logger import FallbackLogger
    from streams.interfaces.abstract_stream_interface import StreamInterface, DEFAULT_EXAMPLE_COUNT
    from streams.stream_builder import StreamBuilder
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...interfaces import (
        StreamInterface, LoggerInterface, LeafConnectorInterface, LeafConnector, Connector, Context, ExtLogger,
        StreamType, LoggingLevel, Message, Name, OptionalFields, Array, Class,
    )
    from ...base.constants.chars import EMPTY, CROP_SUFFIX
    from ...base.constants.text import DEFAULT_LINE_LEN
    from ...base.functions.arguments import get_generated_name, get_cropped_text
    from ...base.abstract.contextual_data import ContextualDataWrapper
    from ...utils.decorators import deprecated_with_alternative
    from ...loggers.fallback_logger import FallbackLogger
    from ..interfaces.abstract_stream_interface import StreamInterface, DEFAULT_EXAMPLE_COUNT
    from ..stream_builder import StreamBuilder

Native = StreamInterface

DATA_MEMBER_NAMES = '_data',


class AbstractStream(ContextualDataWrapper, StreamInterface, ABC):
    def __init__(
            self,
            data: Any,
            name: Optional[Name] = None,
            caption: str = EMPTY,
            source: Connector = None,
            context: Context = None,
            check: bool = False,
    ):
        if name is None:
            if source:
                name = source.get_name()
            else:
                name = get_generated_name()
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
        return DATA_MEMBER_NAMES

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
    def get_class(cls, other: Optional[StreamInterface] = None) -> Class:
        if other is None:
            return StreamBuilder.get_default_stream_class()
        elif isinstance(other, (StreamType, str)):
            return StreamType(other).get_class()
        elif inspect.isclass(other):
            return other
        else:
            raise TypeError(f'"other" argument must be class or StreamType (got {other})')

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
            level: Optional[LoggingLevel] = None,
            end: Optional[str] = None,
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
        meta = self.get_meta()
        return f'with meta {meta}'

    def get_one_line_repr(
            self,
            str_meta: Optional[str] = None,
            max_len: int = DEFAULT_LINE_LEN,
            crop: str = CROP_SUFFIX,
    ) -> str:
        if str_meta is None:
            str_meta = self.get_str_meta()
        cls_name = self.__class__.__name__
        obj_name = self.get_name()
        template = '{cls}({name}, {meta})'
        line = template.format(cls=cls_name, name=obj_name, meta=str_meta)
        if len(line) > max_len:
            max_meta_len = len(template.format(cls=cls_name, name=obj_name, meta=EMPTY))
            str_meta = get_cropped_text(str_meta, max_len=max_meta_len, crop_suffix=crop)
            line = template.format(cls=cls_name, name=obj_name, meta=str_meta)
        return line

    def __repr__(self):
        return self.get_brief_repr()

    def __str__(self):
        return self.get_one_line_repr()

    def _get_demo_example(
            self,
            count: int = DEFAULT_EXAMPLE_COUNT,
            filters: Optional[Array] = None,
            columns: Optional[Array] = None,
            example: Optional[StreamInterface] = None,
    ) -> Native:
        if hasattr(self, 'is_in_memory'):  # isinstance(self, IterableStream)
            is_in_memory = self.is_in_memory()
        else:
            is_in_memory = True  # ?
        if example is not None:
            stream = example
        elif is_in_memory:
            stream = self
        else:  # data is iterator
            stream = self.copy()
        if filters:
            stream = stream.filter(*filters)
        if count is not None:
            stream = stream.take(count)
        if columns and hasattr(stream, 'select'):
            stream = stream.select(columns)
        return stream.collect()

    def show(self, *args, **kwargs):
        display = self.get_display()
        display.display_paragraph(self.get_name(), level=2)
        records, columns = self._get_demo_records_and_columns(*args, **kwargs)
        display.display_sheet(records, columns=columns)
        return self

    @staticmethod
    def _assume_native(stream) -> Native:
        return stream
