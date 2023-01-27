from abc import ABC
from typing import Optional

try:  # Assume we're a submodule in a package.
    from loggers.logger_interface import LoggerInterface
    from base.classes.auto import Auto
    from base.interfaces.sourced_interface import SourcedInterface
    from base.abstract.named import AbstractNamed
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...loggers.logger_interface import LoggerInterface
    from ..classes.auto import Auto
    from ..interfaces.sourced_interface import SourcedInterface
    from ..abstract.named import AbstractNamed

Source = Optional[AbstractNamed]


class SourcedMixin(ABC):
    def get_source(self) -> Source:
        return self._source

    def set_source_inplace(self, source: Source):
        self._source = source

    def set_source(
            self,
            source: Source,
            reset: bool = True,
            inplace: bool = True,
            register: bool = True,
    ) -> SourcedInterface:
        if inplace:
            if reset or not self.get_source():
                self.set_source_inplace(source)
            if register:
                self.register_in_source()
            return self
        else:
            sourced_obj = self.set_outplace(source=source)
            assert isinstance(sourced_obj, SourcedInterface)
            sourced_obj.register()
            return sourced_obj

    def register_in_source(self, check: bool = True) -> SourcedInterface:
        source = self.get_source()
        while hasattr(source, 'get_source') and not hasattr(source, 'get_child'):
            source = source.get_source()
        assert Auto.is_defined(source, check_name=False), 'source for register must be defined'
        assert hasattr(source, 'get_child'), f'expected source as Source, got {source}'
        name = self.get_name()
        known_child = source.get_child(name)
        if known_child is not None:
            if check:
                known_key = known_child.get_key_member_values()
                current_key = self.get_key_member_values()
                assert known_child == self, f'{known_key} != {current_key}'
        else:
            source.add_child(self)
        return self

    def get_logger(self, skip_missing: bool = False) -> Optional[LoggerInterface]:
        source = self.get_source()
        if source:
            if hasattr(source, 'get_logger'):
                return source.get_logger()
        if not skip_missing:
            raise ValueError(f'object {self} has no logger')

    def log(self, *args, **kwargs):
        logger = self.get_logger()
        if logger:
            return logger.log(*args, **kwargs)
