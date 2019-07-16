import subprocess
import logging

log = logging.getLogger('prairiedog')


def test_dgraph_install():
    r = subprocess.run(
        ['dgraph', '-h'], check=True, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    log.debug(r)
    assert True
