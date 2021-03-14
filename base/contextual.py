from abc import ABC, abstractmethod
from typing import Optional, Union, NoReturn

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.named import AbstractNamed
    from base.sourced import Sourced
    from base.context_interface import ContextInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from .named import AbstractNamed
    from .sourced import Sourced
    from .context_interface import ContextInterface


Context = Optional[ContextInterface]
Source = Union[Context, Sourced]


class ContextualInterface(Sourced, ABC):
    @abstractmethod
    def get_source(self) -> Source:
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


class Contextual(ContextualInterface):
    def __init__(
            self, name: str = arg.DEFAULT,
            source: Source = None,
            context: Context = None,
            check: bool = True,
    ):
        name = arg.undefault(name, arg.get_generated_name(self._get_default_name_prefix()))
        if context:
            if source:
                source.set_context(context)
            else:
                source = context
        super().__init__(name=name, source=source, check=check)
        if context:
            self.put_into_context(check=check)

    def get_source(self) -> Union[Source, ContextualInterface]:
        source = self._source
        if source:
            assert isinstance(source, (ContextInterface, ContextualInterface))
        return source

    def _has_context_as_source(self):
        source = self.get_source()
        if hasattr(source, 'is_context'):
            return source.is_context()
        return False

    def get_context(self) -> Context:
        source = self.get_source()
        if self._has_context_as_source():
            return source
        if hasattr(source, 'get_context'):
            return source.get_context()

    def set_context(self, context: ContextInterface, reset=True, inplace=True):
        if inplace:
            if self._has_context_as_source():
                if reset:
                    assert isinstance(context, ContextInterface)
                    assert isinstance(context, AbstractNamed)
                    self.set_source(context)
            elif self.get_source():
                self.get_source().set_context(context, reset=reset)
            self.put_into_context()
        else:
            return self.set_outplace(context=context)

    def put_into_context(self, check=True):
        context = self.get_context()
        assert context, 'for put_into_context context must be defined'
        known_child = context.get_child(self.get_name())
        if known_child:
            if check:
                assert known_child == self, '{} != {}'.format(known_child, self)
        else:
            context.add_child(self)
