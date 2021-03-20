from abc import ABC, abstractmethod
from typing import Optional, NoReturn

try:  # Assume we're a sub-module in a package.
    from base.interfaces.base_interface import BaseInterface
    from base.interfaces.sourced_interface import SourcedInterface
    from base.interfaces.context_interface import ContextInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .base_interface import BaseInterface
    from .sourced_interface import SourcedInterface
    from .context_interface import ContextInterface

Context = Optional[ContextInterface]


class ContextualInterface(SourcedInterface, ABC):
    @abstractmethod
    def get_source(self) -> BaseInterface:
        pass

    @abstractmethod
    def get_context(self) -> Context:
        pass

    @abstractmethod
    def set_context(self, context: Context) -> NoReturn:
        pass

    @abstractmethod
    def put_into_context(self) -> NoReturn:
        pass
