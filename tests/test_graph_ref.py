import pytest
import logging
import datetime

from prairiedog.graph_ref import SubgraphRef
from prairiedog.kmers import Kmers

log = logging.getLogger("prairiedog")


def test_subgraph_creation(g):
    # Create the graph from a shortened file for testing
    km = Kmers("tests/GCA_900015695.1_ED647_contigs_genomic_SHORTENED.fna")
    sgr = SubgraphRef(
        0, km, g
    )
    # kmers directly next to each other on the same contig
    n1 = sgr.subgraph_kmer_map["GCTGGATACGT"]
    n2 = sgr.subgraph_kmer_map["CTGGATACGTA"]
    nodes = sgr.graph.nodes
    edges = sgr.graph.edges
    assert n1 in nodes
    assert n2 in nodes
    assert (n1, n2) in edges

    # kmers directly next to each other further on in a contig
    n3 = sgr.subgraph_kmer_map["AAACTCCAGAG"]
    n4 = sgr.subgraph_kmer_map["AACTCCAGAGT"]
    assert n3 in nodes
    assert n4 in nodes
    assert (n3, n4) in edges

    # kmers on separate contigs should not be connected
    n5 = sgr.subgraph_kmer_map["TACTGCTACTG"]
    n6 = sgr.subgraph_kmer_map["TAACGGTATTT"]
    assert n5 in nodes
    assert n6 in nodes
    assert (n5, n6) not in edges

    # kmers at the end of a contig
    n7 = sgr.subgraph_kmer_map["TTGAGTTTCGG"]
    n8 = sgr.subgraph_kmer_map["TGAGTTTCGGG"]
    assert n7 in nodes
    assert n8 in nodes
    assert (n7, n8) in edges
