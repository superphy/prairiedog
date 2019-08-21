import logging

from prairiedog.graph import Graph
from prairiedog.errors import GraphException
from prairiedog.node import concat_values

log = logging.getLogger("prairiedog")

#####
# Tests against a pre-built database
# Made from:
# - SRR1060582_SHORTENED.fasta,
# - SRR1106609_SHORTENED.fasta
# - GCA_900015695.1_ED647_contigs_genomic_SHORTENED.fasta
#####


def test_lemongraph_connected(prebuilt_graph: Graph):
    connected, starting_edges = prebuilt_graph.connected(
        'CCGGAAGAAAA', 'CGGAAGAAAAA')
    if not connected:
        raise GraphException(prebuilt_graph)
    else:
        assert True
    assert len(starting_edges) == 1
    log.debug("Found starting_edges as {}".format(starting_edges[0]))


def test_lemongraph_connected_path(prebuilt_graph: Graph):
    paths, _ = prebuilt_graph.path('CCGGAAGAAAA', 'CGGAAGAAAAA')
    assert len(paths) == 1

    path = paths[0]
    assert path[0].value == 'CCGGAAGAAAA'
    assert path[1].value == 'CGGAAGAAAAA'

    kmer = concat_values(path)
    assert kmer == 'CCGGAAGAAAAA'


def test_lemongraph_connected_distant(prebuilt_graph: Graph):
    connected, starting_edges = prebuilt_graph.connected('ATACGACGCCA', 'CGTCCGGACGT')
    if not connected:
        raise GraphException(prebuilt_graph)
    else:
        assert True
    assert len(starting_edges) == 1
    log.debug("Found starting_edges as {}".format(starting_edges[0]))


def test_lemongraph_connected_distant_path(prebuilt_graph: Graph):
    paths, _ = prebuilt_graph.path('ATACGACGCCA', 'CGTCCGGACGT')
    assert len(paths) == 1

    path = paths[0]
    assert path[0].value == 'ATACGACGCCA'
    assert path[-1].value == 'CGTCCGGACGT'

    kmer = concat_values(path)
    assert kmer == 'ATACGACGCCAGCGAACGTCCGGACGT'


def test_lemongraph_not_connected(prebuilt_graph: Graph):
    connected, starting_edges = prebuilt_graph.connected(
        'GCTGGATACGT', 'CGTCCGGACGT')
    if connected:
        raise GraphException(prebuilt_graph)
    else:
        assert True
    assert len(starting_edges) == 0


def test_lemongraph_not_connected_path(prebuilt_graph: Graph):
    paths, _ = prebuilt_graph.path('GCTGGATACGT', 'CGTCCGGACGT')
    assert len(paths) == 0
