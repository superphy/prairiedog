import subprocess
import logging

from prairiedog.node import Node
from prairiedog.edge import Edge

log = logging.getLogger('prairiedog')


def test_dgraph_install():
    r = subprocess.run(
        ['dgraph', '-h'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out = str(r)
    assert 'Usage' in out


def test_dgraph_conftest(dg):
    assert True


def test_dgraph_exists_node(dg):
    na = Node(node_type='n', value='a')
    exists, _ = dg.exists_node(na)
    assert not exists
    dg.upsert_node(na, echo=False)
    exists, uid = dg.exists_node(na)
    assert exists
    log.info("uid: {}".format(uid))


def test_dgraph_exists_edge(dg):
    na = Node(value="ATCG")
    nb = Node(value="ATCC")
    e = Edge(src="ATCG", tgt="ATCC")
    exists, _ = dg.exists_edge(e)
    assert not exists
