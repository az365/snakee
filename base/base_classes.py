try:  # Assume we're a submodule in a package.
    from base.interfaces.base_interface import BaseInterface
    from base.interfaces.sourced_interface import SourcedInterface
    from base.interfaces.data_interface import SimpleDataInterface
    from base.interfaces.tree_interface import TreeInterface
    from base.interfaces.context_interface import ContextInterface
    from base.abstract.abstract_base import AbstractBaseObject
    from base.abstract.named import AbstractNamed
    from base.abstract.sourced import Sourced
    from base.abstract.simple_data import SimpleDataWrapper
    from base.abstract.tree_item import TreeItem
    from base.mixin.display_mixin import DisplayMixin
    from base.mixin.data_mixin import DataMixin, IterDataMixin, MapDataMixin, MultiMapDataMixin
    from base.mixin.iterable_mixin import IterableInterface, IterableMixin
    from base.classes.auto import Auto, AUTO
    from base.classes.enum import EnumItem, DynamicEnum, ClassType, SubclassesType
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .interfaces.base_interface import BaseInterface
    from .interfaces.sourced_interface import SourcedInterface
    from .interfaces.data_interface import SimpleDataInterface
    from .interfaces.tree_interface import TreeInterface
    from .interfaces.context_interface import ContextInterface
    from .abstract.abstract_base import AbstractBaseObject
    from .abstract.named import AbstractNamed
    from .abstract.sourced import Sourced
    from .abstract.simple_data import SimpleDataWrapper
    from .abstract.tree_item import TreeItem
    from .mixin.display_mixin import DisplayMixin
    from .mixin.data_mixin import DataMixin, IterDataMixin, MapDataMixin, MultiMapDataMixin
    from .mixin.iterable_mixin import IterableInterface, IterableMixin
    from .classes.auto import Auto, AUTO
    from .classes.enum import EnumItem, DynamicEnum, ClassType, SubclassesType
