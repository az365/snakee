from abc import ABC
from typing import Optional, Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.interfaces.context_interface import ContextInterface
    from base.interfaces.contextual_interface import ContextualInterface
    from base.abstract.named import AbstractNamed
    from base.abstract.sourced import Sourced
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ..interfaces.context_interface import ContextInterface
    from ..interfaces.contextual_interface import ContextualInterface
    from .named import AbstractNamed
    from .sourced import Sourced

Context = Optional[ContextInterface]
Source = Union[Context, Sourced]


class Contextual(Sourced, ContextualInterface, ABC):
    def __init__(
            self, name: str = arg.DEFAULT,
            source: Source = None,
            context: Context = None,
            check: bool = True,
    ):
        name = arg.undefault(name, arg.get_generated_name(self._get_default_name_prefix()))
        if arg.is_defined(context):
            if arg.is_defined(source):
                source.set_context(context)
            else:
                source = context
        super().__init__(name=name, source=source, check=check)
        if arg.is_defined(self.get_context()):
            self.put_into_context(check=check)

    def get_source(self) -> Union[Source, ContextualInterface]:
        source = self._source
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

    def set_context(self, context: ContextInterface, reset: bool = True, inplace: bool = True):
        if inplace:
            if self._has_context_as_source():
                if reset:
                    assert isinstance(context, ContextInterface)
                    assert isinstance(context, AbstractNamed)
                    self.set_source(context)
            elif arg.is_defined(self.get_source()):
                self.get_source().set_context(context, reset=reset)
            self.put_into_context()
        else:
            return self.set_outplace(context=context)

    def put_into_context(self, check: bool = True):
        context = self.get_context()
        if context:
            known_child = context.get_child(self.get_name())
            if known_child:
                if check:
                    if known_child != self:
                        message = 'Object with name {} already registered in context ({} != {})'
                        raise ValueError(message.format(self.get_name(), known_child, self))
            else:
                context.add_child(self)
        elif check:
            msg = 'for put_into_context context must be defined (got object {} with source {}'
            raise ValueError(msg.format(self, self.get_source()))
        return self
