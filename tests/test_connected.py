import pytest
import logging

from prairiedog.lemon_graph import LGGraph
from prairiedog.graph import Graph
from prairiedog.node import Node, concat_values
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


def test_lemongraph_connected_path(lg: LGGraph):
    paths, _ = lg.path('CCGGAAGAAAA', 'CGGAAGAAAAA')
    assert len(paths) == 1

    path = paths[0]
    assert path[0].value == 'CCGGAAGAAAA'
    assert path[1].value == 'CGGAAGAAAAA'

    kmer = concat_values(path)
    assert kmer == 'CCGGAAGAAAAA'


def test_lemongraph_connected_distant(lg: LGGraph):
    connected, starting_edges = lg.connected('ATACGACGCCA', 'CGTCCGGACGT')
    if not connected:
        raise GraphException(lg)
    else:
        assert True
    assert len(starting_edges) == 1
    log.debug("Found starting_edges as {}".format(starting_edges[0]))

def test_lemongraph_connected_distant_path(lg: LGGraph):
    paths, _ = lg.path('ATACGACGCCA', 'CGTCCGGACGT')
    assert len(paths) == 1

    path = paths[0]
    assert path[0].value == 'ATACGACGCCA'
    assert path[-1].value == 'CGTCCGGACGT'

    kmer = concat_values(path)
    assert kmer == 'ATACGACGCCAGCGAACGTCCGGACGT'


def test_lemongraph_not_connected(lg: LGGraph):
    connected, starting_edges = lg.connected('GCTGGATACGT', 'CGTCCGGACGT')
    if connected:
        raise GraphException(lg)
    else:
        assert True
    assert len(starting_edges) == 0


def test_lemongraph_not_connected_path(lg: LGGraph):
    paths, _ = lg.path('GCTGGATACGT', 'CGTCCGGACGT')
    assert len(paths) == 0


#####
# Tests against a fresh database
#####


def _setup_connected(g: Graph):
    n1 = Node(value="ABC")
    n2 = Node(value="BCD")
    g.upsert_node(n1)
    g.upsert_node(n2)
    e = Edge(src="ABC", tgt="BCD", edge_value=0)
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
    paths, _ = g.path("ABC", "BCD")
    # There should be 1 path in paths
    assert len(paths) == 1
    path = paths[0]
    if path[0].value != "ABC" or path[1].value != "BCD":
        raise GraphException(g=g)
    else:
        assert True

    joined = concat_values(path)
    assert joined == "ABCD"


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


def test_graph_not_connected_path(g: Graph):
    _setup_not_connected(g)

    paths, _ = g.path('ABC', 'BCD')
    assert len(paths) == 0


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


def test_graph_connected_no_node_path(g: Graph):
    _setup_connected_no_node(g)

    paths, _ = g.path('ABC', 'BCD')
    assert len(paths) == 0


def _setup_connected_distant(g: Graph):
    n1 = Node(value="ABC")
    n2 = Node(value="BCD")
    n3 = Node(value="CDE")
    g.upsert_node(n1)
    g.upsert_node(n2)
    g.upsert_node(n3)
    e1 = Edge(src="ABC", tgt="BCD", edge_value=0)
    g.add_edge(e1)
    e2 = Edge(src="BCD", tgt="CDE", edge_value=1)
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


def test_graph_connected_distant_path(g: Graph):
    _setup_connected_distant(g)

    paths, _ = g.path('ABC', 'CDE')
    assert len(paths) == 1

    path = paths[0]
    assert len(path) == 3
    assert path[0].value == "ABC"
    assert path[1].value == "BCD"
    assert path[2].value == "CDE"

    joined = concat_values(path)
    assert joined == "ABCDE"


def _setup_connected_multiple(g: Graph):
    n1 = Node(value="ABC")
    n2 = Node(value="BCD")
    n2_alt = Node(value="XYZ")
    n3 = Node(value="CDE")
    g.upsert_node(n1)
    g.upsert_node(n2)
    g.upsert_node(n3)
    g.upsert_node(n2_alt)
    e1 = Edge(src="ABC", tgt="BCD", edge_type="path1", edge_value=0)
    g.add_edge(e1)
    e2 = Edge(src="BCD", tgt="CDE", edge_type="path1", edge_value=1)
    g.add_edge(e2)
    e1_alt = Edge(src="ABC", tgt="XYZ", edge_type="path2", edge_value=0)
    g.add_edge(e1_alt)
    e2_alt = Edge(src="XYZ", tgt="CDE", edge_type="path2", edge_value=1)
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


def test_graph_connected_multiple_path(g: Graph):
    _setup_connected_multiple(g)

    paths, _ = g.path('ABC', 'CDE')
    assert len(paths) == 2
    flagged_bcd = False
    flagged_xyz = False
    for path in paths:
        assert path[0].value == "ABC"
        assert path[2].value == "CDE"
        # flagged to prevent reuse of same value
        if path[1].value == "BCD" and not flagged_bcd:
            flagged_bcd = True
            assert True
        elif path[1].value == "XYZ" and not flagged_xyz:
            flagged_xyz = True
            assert True
        else:
            raise GraphException(g)


def _setup_connected_shortcut(g: Graph):
    n1 = Node(value="ABC")
    n2 = Node(value="BCD")
    n3 = Node(value="CDE")
    g.upsert_node(n1)
    g.upsert_node(n2)
    g.upsert_node(n3)
    e1 = Edge(src="ABC", tgt="BCD", edge_type="path1", edge_value=0)
    g.add_edge(e1)
    e2 = Edge(src="BCD", tgt="CDE", edge_type="path1", edge_value=1)
    g.add_edge(e2)
    e1_short = Edge(src="ABC", tgt="CDE", edge_type="path2", edge_value=0)
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


def test_graph_connected_shortcut_path(g: Graph):
    _setup_connected_shortcut(g)

    paths, _ = g.path('ABC', 'CDE')
    assert len(paths) == 2

    flagged_shortcut = False
    flagged_regular = False
    for path in paths:
        assert path[0].value == "ABC"
        assert path[-1].value == "CDE"
        if len(path) == 2 and not flagged_shortcut:
            # This is the shortcut
            flagged_shortcut = True
            assert True
        elif len(path) == 3 and not flagged_regular:
            flagged_regular = True
            assert path[1].value == "BCD"
        else:
            raise GraphException(g)


def test_graph_connected_repeats_full_path(g: Graph):
    n1 = Node(value="ABC")
    n2 = Node(value="BCD")
    n3 = Node(value="CDE")
    g.upsert_node(n1)
    g.upsert_node(n2)
    g.upsert_node(n3)
    e1 = Edge(src="ABC", tgt="BCD", edge_value=0)
    g.add_edge(e1)
    e2 = Edge(src="BCD", tgt="CDE", edge_value=1)
    g.add_edge(e2)
    e3 = Edge(src="CDE", tgt="ABC", edge_value=2)
    g.add_edge(e3)
    e4 = Edge(src="ABC", tgt="BCD", edge_value=3)
    g.add_edge(e4)
    e5 = Edge(src="BCD", tgt="CDE", edge_value=4)
    g.add_edge(e5)
    g.save()

    try:
        paths, _ = g.path('ABC', 'CDE')
    except:
        raise GraphException(g)

    assert len(paths) == 3

    c = 0
    for path in paths:
        assert path[0].value == "ABC"
        assert path[-1].value == "CDE"
        if len(path) == 3:
            assert path[1].value == "BCD"
            # There are 2 copies of this
            c += 1
        elif len(path) == 6:
            assert path[1].value == "BCD"
            assert path[2].value == "CDE"
            assert path[3].value == "ABC"
            assert path[4].value == "BCD"
            assert path[5].value == "CDE"

    assert c == 2

def test_graph_connected_repeats_end_path(g: Graph):
    n1 = Node(value="ABC")
    n2 = Node(value="BCD")
    n3 = Node(value="CDE")
    g.upsert_node(n1)
    g.upsert_node(n2)
    g.upsert_node(n3)
    e1 = Edge(src="ABC", tgt="BCD", edge_value=0)
    g.add_edge(e1)
    e2 = Edge(src="BCD", tgt="CDE", edge_value=1)
    g.add_edge(e2)
    e3 = Edge(src="CDE", tgt="BCD", edge_value=2)
    g.add_edge(e3)
    e4 = Edge(src="BCD", tgt="CDE", edge_value=3)
    g.add_edge(e4)
    g.save()

    paths, _ = g.path('ABC', 'CDE')
    assert len(paths) == 2


def test_graph_connected_repeats_start_path(g: Graph):
    n1 = Node(value="ABC")
    n2 = Node(value="BCD")
    n3 = Node(value="CDE")
    g.upsert_node(n1)
    g.upsert_node(n2)
    g.upsert_node(n3)
    e1 = Edge(src="ABC", tgt="BCD", edge_value=0)
    g.add_edge(e1)
    e2 = Edge(src="BCD", tgt="ABC", edge_value=1)
    g.add_edge(e2)
    e3 = Edge(src="ABC", tgt="BCD", edge_value=2)
    g.add_edge(e3)
    e4 = Edge(src="BCD", tgt="CDE", edge_value=3)
    g.add_edge(e4)
    g.save()

    paths, _ = g.path('ABC', 'CDE')
    assert len(paths) == 2


def test_graph_connected_repeats_one_middle_path(g: Graph):
    n1 = Node(value="ABC")
    n2 = Node(value="BCD")
    n3 = Node(value="CDE")
    g.upsert_node(n1)
    g.upsert_node(n2)
    g.upsert_node(n3)
    e1 = Edge(src="ABC", tgt="BCD", edge_value=0)
    g.add_edge(e1)
    e2 = Edge(src="BCD", tgt="BCD", edge_value=1)
    g.add_edge(e2)
    e3 = Edge(src="BCD", tgt="CDE", edge_value=2)
    g.add_edge(e3)
    g.save()

    paths, _ = g.path('ABC', 'CDE')
    assert len(paths) == 1


def test_graph_connected_repeats_one_long_path(g: Graph):
    n1 = Node(value="ABC")
    n2 = Node(value="BCD")
    n3 = Node(value="CDE")
    n4 = Node(value="DEF")
    g.upsert_node(n1)
    g.upsert_node(n2)
    g.upsert_node(n3)
    g.upsert_node(n4)
    e1 = Edge(src="ABC", tgt="BCD", edge_value=0)
    g.add_edge(e1)
    e2 = Edge(src="BCD", tgt="CDE", edge_value=1)
    g.add_edge(e2)
    e3 = Edge(src="CDE", tgt="BCD", edge_value=2)
    g.add_edge(e3)
    e4 = Edge(src="BCD", tgt="CDE", edge_value=3)
    g.add_edge(e4)
    e4 = Edge(src="CDE", tgt="DEF", edge_value=4)
    g.add_edge(e4)
    g.save()

    paths, _ = g.path('ABC', 'DEF')
    assert len(paths) == 1
