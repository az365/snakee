import inspect
from abc import ABC, abstractmethod

try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as sm
    from utils import arguments as arg
    from loggers import logger_classes as log
    from loggers.logger_classes import deprecated_with_alternative
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import stream_classes as sm
    from ...utils import arguments as arg
    from ...loggers import logger_classes as log
    from ...loggers.logger_classes import deprecated_with_alternative


DATA_MEMBERS = ['data']


class AbstractStream(ABC):
    def __init__(
            self,
            data,
            name=arg.DEFAULT,
            source=None,
            context=None,
    ):
        self.data = data
        self.source = source
        if source:
            name = arg.undefault(name, source.get_name())
        else:
            name = arg.undefault(name, sm.generate_name())
        self.name = name
        if not context:
            context = sm.get_context()
        self.context = context
        if context is not None:
            self.put_into_context()

    def get_data(self):
        return self.data

    @staticmethod
    def get_data_members():
        return DATA_MEMBERS

    @abstractmethod
    def get_items(self):
        pass

    def get_name(self):
        return self.name

    def set_name(self, name, register=True):
        if register:
            old_name = self.get_name()
            self.context.rename_stream(old_name, name)
        self.name = name
        return self

    def get_context(self):
        return self.context

    def put_into_context(self, name=arg.DEFAULT):
        assert self.context, 'for put_into_context context must be defined'
        name = arg.undefault(name, self.name)
        if name not in self.context.stream_instances:
            self.context.stream_instances[name] = self

    def get_links(self):
        if self.source is not None:
            yield self.source

    def get_meta(self, ex=None):
        meta = self.__dict__.copy()
        for m in self.get_data_members():
            meta.pop(m)
        if isinstance(ex, str):
            meta.pop(ex)
        elif isinstance(ex, (list, tuple)):
            for m in ex:
                meta.pop(m)
        return meta

    def set_meta(self, **meta):
        return self.__class__(
            self.data,
            **meta
        )

    def update_meta(self, **meta):
        return self.stream(
            self.get_data(),
            **meta
        )

    def fill_meta(self, check=True, **meta):
        props = self.get_meta()
        if check:
            unsupported = [k for k in meta if k not in props]
            assert not unsupported, 'class {} does not support these properties: {}'.format(
                self.get_stream_type(),
                unsupported,
            )
        for key, value in props.items():
            if value is None or value == arg.DEFAULT:
                props[key] = meta.get(key)
        return self.__class__(
            self.data,
            **props
        )

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

    def get_mapped_items(self, function):
        return map(function, self.get_items())

    def map(self, function):
        return self.stream(
            self.get_mapped_items(function),
        )

    def get_filtered_items(self, function):
        return filter(function, self.get_items())

    def filter(self, function):
        return self

    def apply_to_data(self, function, *args):
        return self.__class__(
            data=function(self.get_data()),
            **self.get_meta()
        )

    def apply_to_stream(self, function, *args, **kwargs):
        return function(self, *args, **kwargs)

    def apply(self, function, *args, to_stream=False, **kwargs):
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

    def stream(self, data, **kwargs):
        meta = self.get_meta()
        meta.update(kwargs)
        return self.__class__(
            data,
            **meta
        )

    def write_to(self, connector, verbose=True, return_stream=True):
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

    def get_description(self):
        return 'with meta {}'.format(self.get_meta())

    def __str__(self):
        title = '{cls}({name})'.format(cls=self.__class__.__name__, name=self.get_name())
        description = self.get_description()
        if description:
            return '<{title} {desc}>'.format(title=title, desc=description)
        else:
            return '<{}>'.format(title)

    @abstractmethod
    def get_demo_example(self, **kwargs):
        pass

    def show(self, **kwargs):
        self.log(str(self), end='\n', verbose=True, force=True)
        demo_example = self.get_demo_example(**kwargs)
        self.log(demo_example, verbose=False)
        return demo_example
