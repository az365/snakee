from abc import ABC, abstractmethod
from typing import Optional, Union, Iterable, Callable, Any, NoReturn
import inspect
import gc

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.abstract.contextual_data import ContextualDataWrapper
    from streams.interfaces.abstract_stream_interface import StreamInterface
    from streams import stream_classes as sm
    from loggers import logger_classes as log
    from loggers.logger_classes import deprecated_with_alternative
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...base.abstract.contextual_data import ContextualDataWrapper
    from ..interfaces.abstract_stream_interface import StreamInterface
    from .. import stream_classes as sm
    from ...loggers import logger_classes as log
    from ...loggers.logger_classes import deprecated_with_alternative

Stream = StreamInterface
OptionalFields = Optional[Union[Iterable, str]]

DATA_MEMBERS = ('_data', )


class AbstractStream(ContextualDataWrapper, StreamInterface, ABC):
    def __init__(
            self,
            data,
            name: Union[str, arg.DefaultArgument] = arg.DEFAULT,
            source=None,
            context=None,
            check=False,
    ):
        if source:
            name = arg.undefault(name, source.get_name())
        else:
            name = arg.undefault(name, arg.get_generated_name())
        if source and not context:
            context = source.get_context()
        if not context:
            context = sm.get_context()
        super().__init__(name=name, data=data, source=source, context=context, check=check)

    def set_name(self, name: str, register: bool = True, inplace: bool = False) -> Optional[Stream]:
        if register:
            old_name = self.get_name()
            self.get_context().rename_stream(old_name, name)
        return super().set_name(name, inplace=inplace)

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

    def set_meta(self, **meta) -> Stream:
        return super().set_meta(**meta)

    def update_meta(self, **meta) -> Stream:
        return super().update_meta(**meta)

    def fill_meta(self, check=True, **meta) -> Stream:
        return super().fill_meta(check=check, **meta)

    def get_property(self, name, *args, **kwargs):
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

    def get_calc(self, function: Callable, *args, **kwargs) -> Any:
        return function(self.get_data(), *args, **kwargs)

    def apply_to_data(self, function: Callable, dynamic=False, *args, **kwargs):
        return self.stream(  # can be file
            self.get_calc(function, *args, **kwargs),
            ex=self._get_dynamic_meta_fields() if dynamic else None,
        )

    def apply_to_stream(self, function: Callable, *args, **kwargs) -> Stream:
        return function(self, *args, **kwargs)

    def apply(self, function: Callable, *args, to_stream: bool = False, **kwargs):
        if to_stream:
            return self.apply_to_stream(function, *args, **kwargs)
        else:
            return self.apply_to_data(function, *args, **kwargs)

    @log.deprecated_with_alternative('get_stream_type()')
    def get_class_name(self):
        return self.__class__.__name__

    @classmethod
    def get_stream_type(cls):
        return sm.StreamType(cls.__name__)

    @classmethod
    def get_class(cls, other=None):
        if other is None:
            return cls
        elif isinstance(other, (sm.StreamType, str)):
            return sm.StreamType(other).get_class()
        elif inspect.isclass(other):
            return other
        else:
            raise TypeError('"other" parameter must be class or StreamType (got {})'.format(type(other)))

    def stream(self, data, ex: OptionalFields = None, **kwargs) -> Stream:
        meta = self.get_meta(ex=ex)
        meta.update(kwargs)
        stream = self.__class__(data, **meta)
        assert isinstance(stream, Stream)
        return stream

    def to_stream(self) -> Stream:
        return self

    def write_to(self, connector, verbose=True, return_stream=True) -> Optional[Stream]:
        msg = 'connector-argument must be an instance of LeafConnector or have write_stream() method'
        assert hasattr(connector, 'write_stream'), msg
        connector.write_stream(self, verbose=verbose)
        if return_stream:
            return connector.to_stream(verbose=verbose).update_meta(**self.get_meta())

    def get_logger(self, skip_missing=True) -> log.ExtendedLoggerInterface:
        if self.get_context():
            logger = self.get_context().get_logger(create_if_not_yet=skip_missing)
        else:
            logger = None
        if not logger:
            logger = log.get_logger()
        return logger

    def log(self, msg, level=arg.DEFAULT, end=arg.DEFAULT, verbose=True, truncate=True, force=True):
        logger = self.get_logger()
        if logger:
            logger.log(
                msg=msg, level=level,
                end=end, verbose=verbose,
                truncate=truncate,
            )
        elif force:
            print(msg)

    def forget(self) -> NoReturn:
        if hasattr(self, 'close'):
            self.close()
        context = self.get_context()
        if context:
            context.forget_child(self)
        gc.collect()

    def get_description(self) -> str:
        return 'with meta {}'.format(self.get_meta())

    def __repr__(self):
        return "{cls}('{name}')".format(cls=self.__class__.__name__, name=self.get_name())

    def __str__(self):
        title = self.__repr__()
        description = self.get_description()
        if description:
            return '<{title} {desc}>'.format(title=title, desc=description)
        else:
            return '<{}>'.format(title)

    @abstractmethod
    def get_demo_example(self, *args, **kwargs):
        pass

    def show(self, *args, **kwargs):
        self.log(str(self), end='\n', verbose=True, truncate=False)
        demo_example = self.get_demo_example(*args, **kwargs)
        self.log(demo_example, verbose=False)
        return demo_example
