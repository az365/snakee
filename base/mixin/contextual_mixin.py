from abc import ABC
from typing import Optional, Union

try:  # Assume we're a submodule in a package.
    from base.interfaces.context_interface import ContextInterface
    from base.abstract.named import AbstractNamed
    from base.mixin.sourced_mixin import SourcedMixin
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..interfaces.context_interface import ContextInterface
    from ..abstract.named import AbstractNamed
    from .sourced_mixin import SourcedMixin

Native = SourcedMixin
Context = Optional[ContextInterface]
Source = Union[Context, SourcedMixin]


class ContextualMixin(ABC):
    def get_source_of_context(self) -> Union[Native, Source]:
        if hasattr(self, 'get_source'):
            source = self.get_source()
        elif hasattr(self, '_source'):
            source = self._source
        else:
            msg = f'get_source_of_context(): Expected Sourced object having get_source() method, got {repr(self)}'
            raise AttributeError(msg)
        return self._assume_native(source)

    def _has_context_as_source(self) -> bool:
        source = self.get_source_of_context()
        if hasattr(source, 'is_context'):
            return source.is_context()
        return False

    def get_context(self) -> Context:
        source = self.get_source_of_context()
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
            elif self.get_source_of_context() is not None:
                self.get_source_of_context().set_context(context, reset=reset)
            else:
                self.set_source(context)
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
            raise ValueError(msg.format(self, self.get_source_of_context()))
        return self

    @staticmethod
    def _is_stub_instance(context) -> bool:
        return hasattr(context, '_method_stub')

    @staticmethod
    def _assume_native(contextual) -> Native:
        return contextual
