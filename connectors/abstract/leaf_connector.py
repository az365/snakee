from abc import ABC, abstractmethod
from typing import Optional, Union

try:  # Assume we're a sub-module in a package.
    from utils import arguments as arg
    from interfaces import LeafConnectorInterface, Context, Stream, AUTO, Auto, AutoBool, AutoConnector
    from connectors.abstract.abstract_connector import AbstractConnector
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...utils import arguments as arg
    from ...interfaces import LeafConnectorInterface, Context, Stream, AUTO, Auto, AutoBool, AutoConnector
    from ..abstract.abstract_connector import AbstractConnector

Native = LeafConnectorInterface
Parent = Union[Context, AbstractConnector]
Links = Optional[dict]

META_MEMBER_MAPPING = dict(_data='streams', _source='parent')
DEFAULT_VERBOSE = True


class LeafConnector(AbstractConnector, LeafConnectorInterface, ABC):
    def __init__(self, name, parent: Parent = None, streams: Links = None, verbose: AutoBool = AUTO):
        self.verbose = DEFAULT_VERBOSE
        super().__init__(name=name, parent=parent, children=streams)
        self.set_verbose(verbose)

    @classmethod
    def _get_meta_member_mapping(cls) -> dict:
        meta_member_mapping = super()._get_meta_member_mapping()
        meta_member_mapping.update(META_MEMBER_MAPPING)
        return meta_member_mapping

    def is_root(self) -> bool:
        return False

    @staticmethod
    def is_storage() -> bool:
        return False

    def is_leaf(self) -> bool:
        return True

    @staticmethod
    def is_folder():
        return False

    @staticmethod
    def has_hierarchy():
        return False

    def is_verbose(self) -> bool:
        return self.verbose

    def set_verbose(self, verbose: AutoBool = True, parent: AutoConnector = arg.AUTO) -> Native:
        parent = arg.acquire(parent, self.get_parent())
        if not arg.is_defined(verbose):
            if hasattr(parent, 'is_verbose'):
                verbose = parent.is_verbose()
            elif hasattr(parent, 'verbose'):
                verbose = parent.verbose
            else:
                verbose = DEFAULT_VERBOSE
        self.verbose = verbose
        return self

    @abstractmethod
    def is_existing(self):
        pass

    def check(self, must_exists=True):
        if must_exists:
            assert self.is_existing(), 'object {} must exists'.format(self.get_name())

    def write_stream(self, stream: Stream, verbose: bool = True):
        return self.from_stream(stream, verbose=verbose)

    @abstractmethod
    def from_stream(self, stream: Stream, verbose: bool = True):
        pass

    @abstractmethod
    def to_stream(self, **kwargs) -> Stream:
        pass
