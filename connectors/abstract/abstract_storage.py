from abc import ABC
from typing import Optional

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from interfaces import ContextInterface, Context, AutoContext, Name
    from connectors.abstract.hierarchic_connector import HierarchicConnector
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...interfaces import ContextInterface, Context, AutoContext, Name
    from .hierarchic_connector import HierarchicConnector

Native = HierarchicConnector

DEFAULT_PATH_DELIMITER = '/'


class AbstractStorage(HierarchicConnector, ABC):
    _default_context: Context = None

    def __init__(self, name: Name, context: Context, verbose: bool = True):
        if not arg.is_defined(context):
            context = self.get_default_context()
        super().__init__(name=name, parent=context, context=context)
        self.verbose = verbose

    def is_root(self):
        return True

    @staticmethod
    def is_storage():
        return True

    @staticmethod
    def is_folder():
        return False

    def get_storage(self):
        return self

    def get_path_prefix(self):
        return ''

    def get_path_delimiter(self):
        return DEFAULT_PATH_DELIMITER

    def get_path(self):
        return self.get_path_prefix()

    def get_path_as_list(self):
        return [self.get_path()]

    def set_context(self, context: AutoContext, reset: bool = False, inplace: bool = True) -> Optional[Native]:
        if not arg.is_defined(context):
            if not self.get_context(skip_missing=True):
                context = self.get_default_context()
        if arg.is_defined(context):
            self.set_parent(context, reset=False, inplace=True)
        if not inplace:
            return self

    def get_context(self, skip_missing: bool = True) -> Context:
        context = super().get_context()
        if isinstance(context, ContextInterface):
            return context
        elif skip_missing:
            context = self.get_default_context()
            self.set_context(context)
            return context

    @classmethod
    def get_default_context(cls) -> Context:
        context = cls._default_context
        if not arg.is_defined(context):
            context_class = cls.get_default_parent_obj_class()
            context = context_class()
        return context

    @classmethod
    def set_default_context(cls, context: ContextInterface) -> None:
        cls._default_context = context
