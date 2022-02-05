from abc import ABC
from typing import Optional

try:  # Assume we're a submodule in a package.
    from utils.arguments import get_generated_name
    from loggers.logger_interface import LoggerInterface, LoggingLevel
    from base.classes.auto import Auto, AUTO
    from base.interfaces.context_interface import ContextInterface
    from base.interfaces.sourced_interface import SourcedInterface
    from base.abstract.named import AbstractNamed
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.arguments import get_generated_name
    from ...loggers.logger_interface import LoggerInterface, LoggingLevel
    from ..classes.auto import Auto, AUTO
    from ..interfaces.context_interface import ContextInterface
    from ..interfaces.sourced_interface import SourcedInterface
    from .named import AbstractNamed

Source = Optional[AbstractNamed]
Logger = Optional[LoggerInterface]

SPECIFIC_MEMBERS = ('_source', )


class Sourced(AbstractNamed, SourcedInterface, ABC):
    def __init__(self, name: str = AUTO, source: Optional[SourcedInterface] = None, check: bool = True):
        name = Auto.acquire(name, get_generated_name(self._get_default_name_prefix()))
        self._source = source
        super().__init__(name=name)
        if Auto.is_defined(source):
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

    def register(self, check: bool = True) -> SourcedInterface:
        source = self.get_source()
        assert Auto.is_defined(source, check_name=False), 'source for register must be defined'
        name = self.get_name()
        known_child = source.get_child(name)
        if known_child is not None:
            if check:
                assert known_child == self, '{} != {}'.format(
                    known_child.get_key_member_values(), self.get_key_member_values(),
                )
        else:
            source.add_child(self)
        return self

    def get_logger(self, skip_missing: bool = False) -> Logger:
        source = self.get_source()
        if source:
            if hasattr(source, 'get_logger'):
                return source.get_logger()
        if not skip_missing:
            raise ValueError('object {} has no logger'.format(self))

    def log(self, *args, **kwargs):
        logger = self.get_logger()
        if logger:
            return logger.log(*args, **kwargs)
