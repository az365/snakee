from abc import ABC
from typing import Optional, Generator

try:  # Assume we're a submodule in a package.
    from utils.arguments import get_generated_name
    from loggers.logger_interface import LoggerInterface, LoggingLevel
    from base.classes.auto import Auto, AUTO
    from base.constants.chars import EMPTY, PY_INDENT, REPR_DELIMITER
    from base.interfaces.context_interface import ContextInterface
    from base.interfaces.sourced_interface import SourcedInterface
    from base.abstract.named import AbstractNamed
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils.arguments import get_generated_name
    from ...loggers.logger_interface import LoggerInterface, LoggingLevel
    from ..classes.auto import Auto, AUTO
    from ..constants.chars import EMPTY, PY_INDENT, REPR_DELIMITER
    from ..interfaces.context_interface import ContextInterface
    from ..interfaces.sourced_interface import SourcedInterface
    from .named import AbstractNamed

Source = Optional[AbstractNamed]
Logger = Optional[LoggerInterface]

SPECIFIC_MEMBERS = ('_source', )
COLS_FOR_META = [
    ('prefix', 3), ('defined', 3),
    ('key', 20), ('value', 30), ('actual_type', 14), ('expected_type', 20), ('default', 20),
]


class Sourced(AbstractNamed, SourcedInterface, ABC):
    def __init__(
            self,
            name: str = AUTO, caption: str = '',
            source: Optional[SourcedInterface] = None,
            check: bool = True,
    ):
        name = Auto.acquire(name, get_generated_name(self._get_default_name_prefix()))
        self._source = source
        super().__init__(name=name, caption=caption)
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
            assert isinstance(sourced_obj, SourcedInterface)
            sourced_obj.register()
            return sourced_obj

    def register(self, check: bool = True) -> SourcedInterface:
        source = self.get_source()
        while hasattr(source, 'get_source') and not hasattr(source, 'get_child'):
            source = source.get_source()
        assert Auto.is_defined(source, check_name=False), 'source for register must be defined'
        assert hasattr(source, 'get_child'), 'expected source as Source, got {}'.format(source)
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

    def get_meta_description(
            self,
            with_title: bool = True,
            with_summary: bool = True,
            prefix: str = PY_INDENT,
            delimiter: str = REPR_DELIMITER,
    ) -> Generator:
        if with_summary:
            count = len(list(self.get_meta_records()))
            yield '{name} has {count} attributes in meta-data:'.format(name=repr(self), count=count)
        yield from self._get_columnar_lines(
            records=self.get_meta_records(),
            columns=COLS_FOR_META,
            with_title=with_title,
            prefix=prefix,
            delimiter=delimiter,
        )
