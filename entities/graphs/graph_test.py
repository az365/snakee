from entities.graphs.node import Node


def test_node_without_graph():
    node = Node(name='node', register=False)
    node.add_edge_inplace('test_edge_01')
    try:
        node.add_edge_to_graph('test_edge_02')
    except ValueError as e:
        assert 'Graph is not defined' in str(e)
    node.add_edge_to_cache('test_edge_03')
    received_edges = node.get_edges_from_cache()
    expected_edges = ['test_edge_01', 'test_edge_03']
    assert received_edges == expected_edges, f'{received_edges} != {expected_edges}'
    expected_str = f"Node('node', caption='', graph=None, edges_cache={expected_edges})"
    assert str(node) == expected_str, f'{node} != {expected_str}'


def main():
    test_node_without_graph()


if __name__ == '__main__':
    main()
