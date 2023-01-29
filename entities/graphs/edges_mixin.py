from abc import ABC
from typing import Union

from base.classes.auto import Auto
from base.functions.arguments import get_name
from base.abstract.simple_data import SimpleDataWrapper
from base.mixin.map_data_mixin import MapDataMixin

Native = Union[SimpleDataWrapper, MapDataMixin]
Edge = SimpleDataWrapper


class EdgesMixin(ABC):
    def _add_edge(self, edge: Edge, check: bool = True, force: bool = False) -> Native:
        edges = self._get_known_edges()
        if edges is None:
            self._clear_edges()
        if check:
            edge_name = get_name(edge, or_callable=False)
            existing_edge = self._get_edge_by_name(edge_name, default=None)
            if existing_edge and not force:
                assert existing_edge == edge, f'{existing_edge} != {edge_name}'
                return self._update_edge_by_name(name=edge_name, edge=edge)
        return self._force_append_edge(edge)

    def _force_append_edge(self, edge: Edge) -> Native:
        self._get_known_edges().append(edge)
        return self

    def _update_edge_by_name(self, edge: Edge, name: str = None) -> Native:
        if not Auto.is_defined(name):
            name = get_name(edge, or_callable=False)
        self._remove_edge_by_name(name)
        return self._force_append_edge(edge)

    def _remove_edge_by_name(self, name: str) -> int:
        edges = self._get_known_edges()
        count = len(edges)
        removed = 0
        i = 0
        while i < count:
            cur_edge = edges[i]
            if get_name(cur_edge) == name:
                edges.pop(i)
                removed += 1
            else:
                i += 1
        return removed

    def _clear_edges(self) -> Native:
        self._set_edges_inplace(list())
        return self
