from abc import ABC
from typing import Optional

try:  # Assume we're a submodule in a package.
    from loggers.logger_interface import LoggerInterface, LoggingLevel
    from base.classes.auto import Auto, AUTO
    from base.constants.chars import EMPTY
    from base.functions.arguments import get_generated_name
    from base.interfaces.sourced_interface import SourcedInterface
    from base.mixin.sourced_mixin import SourcedMixin
    from base.abstract.named import AbstractNamed
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...loggers.logger_interface import LoggerInterface, LoggingLevel
    from ..classes.auto import Auto, AUTO
    from ..constants.chars import EMPTY
    from ..functions.arguments import get_generated_name
    from ..interfaces.sourced_interface import SourcedInterface
    from ..mixin.sourced_mixin import SourcedMixin
    from .named import AbstractNamed

SPECIFIC_MEMBERS = '_source',


# deprecated_with_alternative('ContextualDataWrapper')
class Sourced(AbstractNamed, SourcedMixin, SourcedInterface, ABC):
    def __init__(
            self,
            name: str = AUTO,
            caption: str = EMPTY,
            source: Optional[SourcedInterface] = None,
            check: bool = True,
    ):
        if name == AUTO:
            name = get_generated_name(prefix=self.__class__.__name__)
        self._source = source
        super().__init__(name=name, caption=caption)
        if Auto.is_defined(source):
            self.register(check=check)

    @classmethod
    def _get_meta_member_names(cls) -> list:
        return super()._get_meta_member_names() + list(SPECIFIC_MEMBERS)

    @classmethod
    def _get_key_member_names(cls) -> list:
        return super()._get_key_member_names() + list(SPECIFIC_MEMBERS)
