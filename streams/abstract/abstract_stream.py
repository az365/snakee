import inspect
from datetime import datetime
from abc import ABC, abstractmethod


DATA_MEMBERS = ['data']


try:  # Assume we're a sub-module in a package.
    from streams import stream_classes as sm
    from utils import (
        arguments as arg,
        mappers as ms,
        items as it,
        selection,
        algo,
    )
    from selection import selection_classes as sn
    from loggers import logger_classes as log
    from functions import all_functions as fs
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import stream_classes as sm
    from ...utils import (
        arguments as arg,
        mappers as ms,
        items as it,
        selection,
        algo,
    )
    from ...selection import selection_classes as sn
    from ...loggers import logger_classes as log
    from ...functions import all_functions as fs


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
            name = arg.undefault(name, datetime.now().isoformat())
        self.name = name
        if not context:
            context = sm.get_context()
        self.context = context
        if context is not None:
            self.put_into_context()

    def get_data(self):
        return self.data

    def get_context(self):
        return self.context

    def put_into_context(self, name=arg.DEFAULT):
        assert self.context, 'for put_into_context context must be defined'
        name = arg.undefault(name, self.name)
        if name not in self.context.stream_instances:
            self.context.stream_instances[name] = self

    def get_name(self):
        return self.name

    def set_name(self, name, register=True):
        if register:
            old_name = self.get_name()
            self.context.rename_stream(old_name, name)
        self.name = name
        return self

    def get_logger(self):
        if self.get_context():
            return self.get_context().get_logger()
        else:
            return log.get_logger()

    def log(self, msg, level=arg.DEFAULT, end=arg.DEFAULT, verbose=True):
        logger = self.get_logger()
        if logger is not None:
            logger.log(
                msg=msg, level=level,
                end=end, verbose=verbose,
            )

    def get_links(self):
        if self.source is not None:
            yield self.source

    def get_meta(self):
        meta = self.__dict__.copy()
        for m in DATA_MEMBERS:
            meta.pop(m)
        return meta

    def set_meta(self, **meta):
        return self.__class__(
            self.data,
            **meta
        )

    def update_meta(self, **meta):
        props = self.get_meta()
        props.update(meta)
        return self.__class__(
            self.data,
            **props
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

    def get_class_name(self):
        return self.__class__.__name__

    def get_stream_type(self):
        return sm.StreamType(self.get_class_name())

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

