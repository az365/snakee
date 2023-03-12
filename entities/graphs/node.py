from typing import Optional, Iterable, Union, NoReturn

try:  # Assume we're a submodule in a package.
    from base.constants.chars import EMPTY
    from base.functions.arguments import get_name
    from base.functions.errors import raise_value_error
    from base.abstract.simple_data import SimpleDataWrapper
    from base.mixin.map_data_mixin import MapDataMixin
    from base.mixin.sourced_mixin import SourcedMixin
    from entities.graphs.edges_mixin import EdgesMixin
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from ...base.constants.chars import EMPTY
    from ...base.functions.arguments import get_name
    from ...base.functions.errors import raise_value_error
    from ...base.abstract.simple_data import SimpleDataWrapper
    from ...base.mixin.map_data_mixin import MapDataMixin
    from ...base.mixin.sourced_mixin import SourcedMixin
    from .edges_mixin import EdgesMixin

Native = Union[SimpleDataWrapper, MapDataMixin]
Graph = Union[SimpleDataWrapper, MapDataMixin]  # tmp
Edge = SimpleDataWrapper
Content = Union[SimpleDataWrapper, MapDataMixin, None]

META_MEMBER_MAPPING = dict(_source='graph')


class Node(SimpleDataWrapper, SourcedMixin, EdgesMixin):
    def __init__(
            self,
            data: Content = None,
            name: str = None,
            caption: str = EMPTY,
            graph: Graph = None,
            register: bool = True,
            edges_cache: Optional[list] = None,
    ):
        self._source = graph
        self._edges_cache = edges_cache
        super().__init__(data, name=name, caption=caption)
        if register:
            assert graph is not None  # isinstance(graph, Graph)
            self.register_in_graph()

    @classmethod
    def _get_meta_member_mapping(cls) -> dict:
        meta_member_mapping = super()._get_meta_member_mapping()
        meta_member_mapping.update(META_MEMBER_MAPPING)
        return meta_member_mapping

    def _assert_graph(self, caller=None) -> Union[Native, NoReturn]:
        graph = self._get_graph_if_defined()
        if graph is None:
            if not caller:
                caller = self
            raise_value_error('Graph is not defined', obj=caller)
        return self

    def get_graph(self, skip_missing: bool = False) -> Optional[Graph]:
        if not skip_missing:
            self._assert_graph(self.get_graph)
        return self._get_graph_if_defined()

    def _get_graph_if_defined(self) -> Optional[Graph]:
        return self.get_source()

    def register_in_graph(self, check: bool = True) -> Native:
        self.register_in_source(check=check)
        return self

    def invalidate_edges_cache(self) -> Native:
        self._set_edges_inplace(None)
        return self

    def _set_edges_inplace(self, edges: list):
        self._edges_cache = edges
        return self

    def _get_known_edges(self) -> Optional[list]:
        return self._edges_cache

    def get_edges_from_cache(self) -> Optional[list]:
        return self._edges_cache

    def get_edges_from_graph(self, graph: Optional[Graph] = None) -> Iterable[Edge]:
        if graph is None:
            graph = self.get_graph()
        self._assert_graph(caller=self.get_edges_from_graph)
        return graph.get_edges_for_node(self)

    def get_edges_iter(self, use_cache: bool = True) -> Iterable[Edge]:
        edges_cache = self.get_edges_from_cache()
        if use_cache:
            if edges_cache is not None:
                yield from edges_cache
        else:
            self._assert_graph(caller=self.get_edges_iter)
            for e in self.get_edges_from_graph():
                if e not in edges_cache:
                    edges_cache.append(e)
                yield e

    def _get_edge_by_name(self, name: str, default=None) -> Optional[Edge]:
        for e in self._get_known_edges():
            if get_name(e) == name:
                return e
        if isinstance(default, BaseException):
            raise default
        else:
            return default

    def add_edge_inplace(self, edge: Edge) -> Native:
        graph = self.get_graph(skip_missing=True)
        if graph is None:
            return self.add_edge_to_cache(edge)
        else:
            return self.add_edge_to_graph(edge)

    def add_edge_to_graph(self, edge: Edge) -> Native:
        self._assert_graph(caller=self.add_edge_inplace)
        self.get_graph().add_edge(edge)
        self.invalidate_edges_cache()
        return self

    def add_edge_to_cache(self, edge: Edge, check: bool = True, force: bool = False) -> Native:
        return self._add_edge(edge, check=check, force=force)
