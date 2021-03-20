try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.abstract.abstract_base import AbstractBaseObject
    from base.interfaces.sourced_interface import SourcedInterface
    from base.interfaces.contextual_interface import ContextualInterface
    from base.interfaces.tree_interface import TreeInterface
    from base.interfaces.context_interface import ContextInterface
    from base.abstract.named import AbstractNamed
    from base.abstract.sourced import Sourced
    from base.abstract.contextual import Contextual
    from base.abstract.data import DataWrapper
    from base.abstract.tree_item import TreeItem
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from .abstract.abstract_base import AbstractBaseObject
    from .interfaces.sourced_interface import SourcedInterface
    from .interfaces.contextual_interface import ContextualInterface
    from .interfaces.tree_interface import TreeInterface
    from .interfaces.context_interface import ContextInterface
    from .abstract.named import AbstractNamed
    from .abstract.sourced import Sourced
    from .abstract.contextual import Contextual
    from .abstract.data import DataWrapper
    from .abstract.tree_item import TreeItem
