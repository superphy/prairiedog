import pytest
import logging

from prairiedog.lemon_graph import LGGraph

log = logging.getLogger("prairiedog")


def test_lemongraph(lg: LGGraph):
    connected, path = lg.connected('CCGGAAGAAAA', 'CGGAAGAAAAA')
    assert connected
    log.debug("Found path as {}".format(path))
    # TODO: make this pass
    assert path == set(['somepath'])
