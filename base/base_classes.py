try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from base.abstract_base import AbstractSnakeeBaseObject
    from base.named import AbstractNamed
    from base.sourced import SourcedInterface, Sourced
    from base.contextual import Contextual
    from base.data import DataWrapper
    from base.tree_interface import TreeInterface
    from base.tree_item import TreeItem
    from base.context_interface import ContextInterface
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ..utils import arguments as arg
    from .abstract_base import AbstractSnakeeBaseObject
    from .named import AbstractNamed
    from .sourced import SourcedInterface, Sourced
    from .contextual import ContextInterface, Contextual
    from .data import DataWrapper
    from .tree_interface import TreeInterface
    from .tree_item import TreeItem
    from .context_interface import ContextInterface
