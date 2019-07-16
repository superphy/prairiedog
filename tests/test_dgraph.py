import subprocess
import logging

log = logging.getLogger('prairiedog')


def test_dgraph_install():
    r = subprocess.run(
        ['dgraph', '-h'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    log.debug(r)
    assert 'Usage' in r
