import pytest
import logging

from prairiedog.kmer_graph import KmerGraph

log = logging.getLogger("prairiedog")


def test_kmer_graph_basic(g):
    """Simple check to see if we can load all the genomes.
    """
    kmg = KmerGraph(["tests/GCA_900015695.1_ED647_contigs_genomic_SHORTENED.fna"], g)
    assert isinstance(kmg, KmerGraph)


def test_kmer_graph_benchmark(memory_profiler, all_genome_files, g):
    log.info("Benchmarking with {} genome files".format(len(all_genome_files)))

    def profile():
        kmg = KmerGraph(all_genome_files, g)
        assert isinstance(kmg, KmerGraph)
    mem_usage = memory_profiler.memory_usage(profile)
    log.info("Memory usage was: {} MB".format(max(mem_usage)))


def test_kmer_graph_creation(g):
    # Create the graph from a shortened file for testing
    kmg = KmerGraph(
        ["tests/GCA_900015695.1_ED647_contigs_genomic_SHORTENED.fna"],
        g,
        11
    )
    # kmers directly next to each other on the same contig
    n1 = "GCTGGATACGT"
    n2 = "CTGGATACGTA"
    nodes = kmg.graph.nodes
    edges = kmg.graph.edges
    assert n1 in nodes
    assert n2 in nodes
    assert (n1, n2) in edges

    # kmers directly next to each other further on in a contig
    n3 = "AAACTCCAGAG"
    n4 = "AACTCCAGAGT"
    assert n3 in nodes
    assert n4 in nodes
    assert (n3, n4) in edges

    # kmers on separate contigs should not be connected
    n5 = "TACTGCTACTG"
    n6 = "TAACGGTATTT"
    assert n5 in nodes
    assert n6 in nodes
    assert (n5, n6) not in edges

    # kmers at the end of a contig
    n7 = "TTGAGTTTCGG"
    n8 = "TGAGTTTCGGG"
    assert n7 in nodes
    assert n8 in nodes
    assert (n7, n8) in edges