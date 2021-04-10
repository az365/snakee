from abc import ABC
from typing import Optional, NoReturn

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.abstract.named import AbstractNamed
    from base.interfaces.context_interface import ContextInterface
    from base.interfaces.sourced_interface import SourcedInterface
    from loggers.logger_interface import LoggerInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from .named import AbstractNamed
    from ..interfaces.context_interface import ContextInterface
    from ..interfaces.sourced_interface import SourcedInterface
    from ...loggers.logger_interface import LoggerInterface

Source = Optional[AbstractNamed]
Logger = Optional[LoggerInterface]

SPECIFIC_MEMBERS = ('_source', )


class Sourced(AbstractNamed, SourcedInterface, ABC):
    def __init__(self, name: str = arg.DEFAULT, source: Optional[SourcedInterface] = None, check: bool = True):
        name = arg.undefault(name, arg.get_generated_name(self._get_default_name_prefix()))
        super().__init__(name)
        self._source = source
        if arg.is_defined(source):
            self.register(check=check)

    @classmethod
    def _get_default_name_prefix(cls) -> str:
        return cls.__name__

    @classmethod
    def _get_meta_member_names(cls) -> list:
        return super()._get_meta_member_names() + list(SPECIFIC_MEMBERS)

    @classmethod
    def _get_key_member_names(cls) -> list:
        return super()._get_key_member_names() + list(SPECIFIC_MEMBERS)

    def get_source(self) -> Source:
        return self._source

    def set_source(self, source: Source, reset=True, inplace=True) -> Optional[SourcedInterface]:
        if inplace:
            if reset or not self.get_source():
                self._source = source
            self.register()
        else:
            sourced_obj = self.set_outplace(source=source)
            sourced_obj.register()
            return sourced_obj

    def register(self, check=True):
        source = self.get_source()
        assert source, 'source for register must be defined'
        known_child = source.get_child(self.get_name())
        if known_child:
            if check:
                assert known_child == self, '{} != {}'.format(
                    known_child.get_key_member_values(), self.get_key_member_values(),
                )
        else:
            source.add_child(self)

    def get_logger(self, skip_missing=False) -> Logger:
        source = self.get_source()
        if source:
            if hasattr(source, 'get_logger'):
                return source.get_logger()
        if not skip_missing:
            raise ValueError('object {} has no logger'.format(self))

    def log(self, *args, **kwargs):
        logger = self.get_logger()
        if logger:
            logger.log(*args, **kwargs)
