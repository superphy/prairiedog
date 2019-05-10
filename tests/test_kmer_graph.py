import pytest

from prairiedog.kmer_graph import KmerGraph


def test_kmer_graph_basic(genome_files, g):
    """Simple check to see if we can load all the genomes.
    """
    kmg = KmerGraph(genome_files, g)
    assert isinstance(kmg, KmerGraph)
