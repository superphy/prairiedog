import pytest
import logging

from prairiedog.lemon_graph import LGGraph
from prairiedog.graph import Graph
from prairiedog.node import Node
from prairiedog.edge import Edge
from prairiedog.errors import GraphException

log = logging.getLogger("prairiedog")


#####
# Tests against a pre-created LemonGraph database
#####


def test_lemongraph_connected(lg: LGGraph):
    connected, starting_edges = lg.connected('CCGGAAGAAAA', 'CGGAAGAAAAA')
    if not connected:
        raise GraphException(lg)
    else:
        assert True
    assert len(starting_edges) == 1
    log.debug("Found starting_edges as {}".format(starting_edges[0]))


def test_lemongraph_connected_distant(lg: LGGraph):
    connected, starting_edges = lg.connected('ATACGACGCCA', 'CGTCCGGACGT')
    if not connected:
        raise GraphException(lg)
    else:
        assert True
    assert len(starting_edges) == 1
    log.debug("Found starting_edges as {}".format(starting_edges[0]))


def test_lemongraph_not_connected(lg: LGGraph):
    connected, starting_edges = lg.connected('GCTGGATACGT', 'CGTCCGGACGT')
    if connected:
        raise GraphException(lg)
    else:
        assert True
    assert len(starting_edges) == 0


#####
# Tests against a fresh database
#####


def _setup_connected(g: Graph):
    n1 = Node(value="ABC")
    n2 = Node(value="BCD")
    g.upsert_node(n1)
    g.upsert_node(n2)
    e = Edge(src="ABC", tgt="BCD", incr=0)
    g.add_edge(e)
    g.save()


def test_graph_connected(g: Graph):
    _setup_connected(g)

    connected, starting_edges = g.connected('ABC', 'BCD')
    if not connected:
        raise GraphException(g)
    else:
        assert True
    assert len(starting_edges) == 1
    log.debug("Found starting_edges as {}".format(starting_edges[0]))


def test_graph_connected_path(g: Graph):
    _setup_connected(g)
    paths = g.path("ABC", "BCD")
    if paths[0].value != "ABC" or paths[1].value != "BCE":
        raise GraphException(g=g)
    else:
        assert True


def _setup_not_connected(g: Graph):
    n1 = Node(value="ABC")
    n2 = Node(value="BCD")
    g.upsert_node(n1)
    g.upsert_node(n2)
    g.save()


def test_graph_not_connected(g: Graph):
    _setup_not_connected(g)

    connected, starting_edges = g.connected('ABC', 'BCD')
    if connected:
        raise GraphException(g)
    else:
        assert True
    assert len(starting_edges) == 0


def _setup_connected_no_node(g: Graph):
    n1 = Node(value="ABC")
    g.upsert_node(n1)
    g.save()


def test_graph_connected_no_node(g: Graph):
    _setup_connected_no_node(g)

    connected, starting_edges = g.connected('ABC', 'BCD')
    if connected:
        raise GraphException(g)
    else:
        assert True
    assert len(starting_edges) == 0


def _setup_connected_distant(g: Graph):
    n1 = Node(value="ABC")
    n2 = Node(value="BCD")
    n3 = Node(value="CDE")
    g.upsert_node(n1)
    g.upsert_node(n2)
    g.upsert_node(n3)
    e1 = Edge(src="ABC", tgt="BCD", incr=0)
    g.add_edge(e1)
    e2 = Edge(src="BCD", tgt="CDE", incr=1)
    g.add_edge(e2)
    g.save()


def test_graph_connected_distant(g: Graph):
    _setup_connected_distant(g)

    connected, starting_edges = g.connected('ABC', 'CDE')
    if not connected:
        raise GraphException(g)
    else:
        assert True
    assert len(starting_edges) == 1
    log.debug("Found starting_edges as {}".format(starting_edges[0]))


def _setup_connected_multiple(g: Graph):
    n1 = Node(value="ABC")
    n2 = Node(value="BCD")
    n2_alt = Node(value="XYZ")
    n3 = Node(value="CDE")
    g.upsert_node(n1)
    g.upsert_node(n2)
    g.upsert_node(n3)
    g.upsert_node(n2_alt)
    e1 = Edge(src="ABC", tgt="BCD", edge_type="path", edge_value="path1",
              incr=0)
    g.add_edge(e1)
    e2 = Edge(src="BCD", tgt="CDE", edge_type="path", edge_value="path1",
              incr=1)
    g.add_edge(e2)
    e1_alt = Edge(src="ABC", tgt="XYZ", edge_type="path", edge_value="path2",
                  incr=0)
    g.add_edge(e1_alt)
    e2_alt = Edge(src="XYZ", tgt="CDE", edge_type="path", edge_value="path2",
                  incr=1)
    g.add_edge(e2_alt)
    g.save()


def test_graph_connected_multiple(g: Graph):
    _setup_connected_multiple(g)

    connected, starting_edges = g.connected('ABC', 'CDE')
    if not connected:
        raise GraphException(g)
    else:
        assert True
    assert len(starting_edges) == 2
    log.debug("Found starting_edges as:")
    for e in starting_edges:
        log.debug(e)


def _setup_connected_shortcut(g: Graph):
    n1 = Node(value="ABC")
    n2 = Node(value="BCD")
    n3 = Node(value="CDE")
    g.upsert_node(n1)
    g.upsert_node(n2)
    g.upsert_node(n3)
    e1 = Edge(src="ABC", tgt="BCD", edge_type="path", edge_value="path1",
              incr=0)
    g.add_edge(e1)
    e2 = Edge(src="BCD", tgt="CDE", edge_type="path", edge_value="path1",
              incr=1)
    g.add_edge(e2)
    e1_short = Edge(src="ABC", tgt="CDE", edge_type="path", edge_value="path2",
                    incr=0)
    g.add_edge(e1_short)
    g.save()


def test_graph_connected_shortcut(g: Graph):
    _setup_connected_shortcut(g)

    connected, starting_edges = g.connected('ABC', 'CDE')
    if not connected:
        raise GraphException(g)
    else:
        assert True
    assert len(starting_edges) == 2
    log.debug("Found starting_edges as:")
    for e in starting_edges:
        log.debug(e)
