from abc import ABC
from typing import Optional, Union

try:  # Assume we're a submodule in a package.
    from base.classes.auto import Auto, AUTO
    from base.functions.arguments import get_generated_name
    from base.interfaces.context_interface import ContextInterface
    from base.interfaces.contextual_interface import ContextualInterface
    from base.abstract.named import AbstractNamed
    from base.abstract.sourced import Sourced
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.auto import Auto, AUTO
    from ..functions.arguments import get_generated_name
    from ..interfaces.context_interface import ContextInterface
    from ..interfaces.contextual_interface import ContextualInterface
    from .named import AbstractNamed
    from .sourced import Sourced

Native = ContextualInterface
Context = Optional[ContextInterface]
Source = Union[Context, Sourced]


class Contextual(Sourced, ContextualInterface, ABC):
    def __init__(
            self,
            name: str = AUTO, caption: str = '',
            source: Source = None, context: Context = None,
            check: bool = True,
    ):
        name = Auto.delayed_acquire(name, get_generated_name, self._get_default_name_prefix())
        if Auto.is_defined(context):
            if Auto.is_defined(source):
                source.set_context(context)
            else:
                source = context
        super().__init__(name=name, caption=caption, source=source, check=check)
        if Auto.is_defined(self.get_context()):
            self.put_into_context(check=check)

    def get_source(self) -> Union[Native, Source]:
        source = self._source
        return source

    def _has_context_as_source(self) -> bool:
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

    def set_context(self, context: ContextInterface, reset: bool = True, inplace: bool = True) -> Native:
        if inplace:
            if self._is_stub_instance(context):
                return self
            elif self._has_context_as_source():
                if reset:
                    assert isinstance(context, ContextInterface)
                    assert isinstance(context, AbstractNamed)
                    self.set_source(context)
            elif Auto.is_defined(self.get_source()):
                self.get_source().set_context(context, reset=reset)
            return self.put_into_context(check=not reset)
        else:
            contextual = self.set_outplace(context=context)
            return self._assume_native(contextual)

    def put_into_context(self, check: bool = True) -> Native:
        context = self.get_context()
        if self._is_stub_instance(context):
            pass
        elif context:
            known_child = context.get_child(self.get_name())
            if known_child:
                if check:
                    if known_child != self:
                        message = 'Object with name {} already registered in context ({} != {})'
                        raise ValueError(message.format(self.get_name(), known_child, self))
                else:
                    return context.add_child(self, reset=True, inplace=True) or self
            else:
                return context.add_child(self) or self
        elif check:
            msg = 'for put_into_context context must be defined (got object {} with source {}'
            raise ValueError(msg.format(self, self.get_source()))
        return self

    @staticmethod
    def _is_stub_instance(context) -> bool:
        return hasattr(context, '_method_stub')

    @staticmethod
    def _assume_native(contextual) -> Native:
        return contextual
