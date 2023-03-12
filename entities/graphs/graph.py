from typing import Optional, Iterable, Union

from base.constants.chars import EMPTY
from base.abstract.simple_data import SimpleDataWrapper
from base.mixin.sourced_mixin import SourcedMixin
from entities.graphs.edges_mixin import EdgesMixin

Native = Union[SimpleDataWrapper, SourcedMixin, EdgesMixin]
Source = Native
Edge = SimpleDataWrapper


class Graph(SimpleDataWrapper, SourcedMixin, EdgesMixin):
    def __init__(
            self,
            data=None,
            name: str = None,
            caption: str = EMPTY,
            source: Source = None,
            register: bool = False,
            edges: Optional[list] = None,
    ):
        if edges is None:
            edges = list()
        self._edges = edges
        self._source = source
        super().__init__(data, name=name, caption=caption)
        if register:
            assert source is not None
            self.register_in_source()

    def _get_known_edges(self) -> list:
        return self._data

    def _set_edges_inplace(self, edges: list):
        self._data = edges
        return self

    def get_edges_iter(self) -> Iterable[Edge]:
        return self._get_known_edges()
