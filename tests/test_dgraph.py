import subprocess
import logging

from prairiedog.node import Node

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
    assert not dg.exists_node(na)
    dg.upsert_node(na, echo=False)
    assert dg.exists_node(na)
