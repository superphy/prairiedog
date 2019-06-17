import pytest
import logging

from prairiedog.lemon_graph import LGGraph

log = logging.getLogger("prairiedog")


def test_lemongraph_connected(lg: LGGraph):
    connected, starting_edges = lg.connected('CCGGAAGAAAA', 'CGGAAGAAAAA')
    assert connected
    assert len(starting_edges) == 1
    log.debug("Found starting_edges as {}".format(starting_edges[0]))


def test_lemongraph_connected_distant(lg: LGGraph):
    connected, starting_edges = lg.connected('ATACGACGCCA', 'CGTCCGGACGT')
    assert connected
    assert len(starting_edges) == 1
    log.debug("Found starting_edges as {}".format(starting_edges[0]))


def test_lemongraph_not_connected(lg: LGGraph):
    connected, starting_edges = lg.connected('GCTGGATACGT', 'CGTCCGGACGT')
    assert not connected
    assert len(starting_edges) == 0
