from abc import ABC
from typing import Optional, Union

try:  # Assume we're a submodule in a package.
    from base.classes.auto import Auto, AUTO
    from base.constants.chars import EMPTY
    from base.functions.arguments import get_generated_name
    from base.interfaces.context_interface import ContextInterface
    from base.mixin.contextual_mixin import ContextualMixin
    from base.abstract.sourced import Sourced
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..classes.auto import Auto, AUTO
    from ..constants.chars import EMPTY
    from ..functions.arguments import get_generated_name
    from ..interfaces.context_interface import ContextInterface
    from ..mixin.contextual_mixin import ContextualMixin
    from .sourced import Sourced


class Contextual(Sourced, ContextualMixin, ABC):
    def __init__(
            self,
            name: Union[str, Auto] = AUTO,
            caption: str = EMPTY,
            source: Union[ContextInterface, Sourced, None] = None,
            context: Optional[ContextInterface] = None,
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
