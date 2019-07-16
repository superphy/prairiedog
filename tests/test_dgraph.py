import subprocess
import logging

log = logging.getLogger('prairiedog')


def test_dgraph_install():
    r = subprocess.run(
        ['dgraph', '-h'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out = str(r)
    assert 'Usage' in out


def test_dgraph_conftest(dg):
    assert True
