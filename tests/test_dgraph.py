import subprocess
import logging

from prairiedog.node import Node
from prairiedog.edge import Edge
from prairiedog.dgraph import Dgraph

log = logging.getLogger('prairiedog')


def test_dgraph_install():
    r = subprocess.run(
        ['dgraph', '-h'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out = str(r)
    assert 'Usage' in out


def test_dgraph_conftest(dg: Dgraph):
    assert True

# TODO: this currently runs too slow for tests
# def test_dgraph_preload(dg):
#     dg.preload()
#     assert True


def test_dgraph_exists_node(dg: Dgraph):
    na = Node(node_type='n', value='a')
    exists, _ = dg.exists_node(na)
    assert not exists
    dg.upsert_node(na, echo=False)
    exists, uid = dg.exists_node(na)
    assert exists
    log.info("uid: {}".format(uid))


def test_dgraph_exists_edge(dg: Dgraph):
    e = Edge(src="ATCG", tgt="ATCC", edge_value=1)
    exists, _ = dg.exists_edge(e)
    assert not exists
    dg.upsert_edge(e)
    exists, _ = dg.exists_edge(e)
    assert exists


def test_dgraph_find_value(dg: Dgraph):
    na = Node(value="ATCG")
    nb = Node(value="ATCC")
    dg.upsert_node(na)
    dg.upsert_node(nb)
    e = Edge(src="ATCG", tgt="ATCC", edge_type="genome_a", edge_value=0)
    dg.upsert_edge(e)

    _, uid = dg.exists_node(na)
    v = dg.find_value(uid, "genome_a")
    log.info("Found value for {} as {}".format(uid, v))
    assert v == 0


def test_dgraph_find_value_reverse(dg: Dgraph):
    na = Node(value="ATCG")
    nb = Node(value="ATCC")
    dg.upsert_node(na)
    dg.upsert_node(nb)
    e = Edge(src="ATCG", tgt="ATCC", edge_type="genome_a", edge_value=0)
    dg.upsert_edge(e)

    _, uid_b = dg.exists_node(nb)
    v2 = dg.find_value_reverse(uid_b, "genome_a")
    assert v2 == 0


def test_dgraph_find_depth(dg: Dgraph):
    na = Node(value="ATCG")
    nb = Node(value="ATCC")
    dg.upsert_node(na)
    dg.upsert_node(nb)
    e = Edge(src="ATCG", tgt="ATCC", edge_type="genome_a", edge_value=0)
    dg.upsert_edge(e)
    _, uid_a = dg.exists_node(na)
    _, uid_b = dg.exists_node(nb)
    d = dg.find_depth(uid_a, uid_b, "genome_a")
    log.info("Found depth for {} to {} as {}".format(uid_a, uid_b, d))
    assert d == 1

    nc = Node(value="ATGG")
    dg.upsert_node(nc)
    e2 = Edge(src="ATCC", tgt="ATGG", edge_type="genome_a", edge_value=1)
    dg.upsert_edge(e2)
    _, uid_c = dg.exists_node(nc)
    d2 = dg.find_depth(uid_a, uid_c, "genome_a")
    log.info("Found depth for {} to {} as {}".format(uid_a, uid_c, d))
    assert d2 == 2


def test_dgraph_find_edges(dg: Dgraph):
    na = Node(value="ATCG")
    nb = Node(value="ATCC")
    dg.upsert_node(na)
    dg.upsert_node(nb)
    e = Edge(src="ATCG", tgt="ATCC", edge_type="genome_a", edge_value=0)
    dg.upsert_edge(e)

    ef_set = dg.find_edges("ATCG")
    assert len(ef_set) == 1
    ef = ef_set[0]
    assert ef.src == "ATCG"
    assert ef.tgt == "ATCC"
    assert ef.edge_value == 0
    assert ef.edge_type == "genome_a"


def test_dgraph_find_edges_reverse(dg: Dgraph):
    na = Node(value="ATCG")
    nb = Node(value="ATCC")
    dg.upsert_node(na)
    dg.upsert_node(nb)
    e = Edge(src="ATCG", tgt="ATCC", edge_type="genome_a", edge_value=0)
    dg.upsert_edge(e)

    ef_set = dg.find_edges_reverse("ATCC")
    assert len(ef_set) == 1
    ef = ef_set[0]
    assert ef.src == "ATCG"
    assert ef.tgt == "ATCC"
    assert ef.edge_value == 0
    assert ef.edge_type == "genome_a"
