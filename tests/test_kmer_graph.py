import pytest
import logging
import datetime
import itertools

from prairiedog.kmer_graph import KmerGraph

log = logging.getLogger("prairiedog")


def test_kmer_graph_benchmark(memory_profiler, all_genome_files, g):
    log.info("Benchmarking with {} genome files".format(len(all_genome_files)))

    def profile():
        KmerGraph(all_genome_files, g)
    mem_usage = memory_profiler.memory_usage(profile)
    log.info("Memory usage was: {} MB".format(max(mem_usage)))


def test_kmer_graph_load(monkeypatch, genome_files_shortened):
    subgraphs = []

    # Monkeypatch KmerGraph's call to GraphRef.append so we collect the
    # subgraphs
    def mock_append(self, subgraph):
        subgraphs.append(subgraph)
    monkeypatch.setattr('prairiedog.graph_ref.GraphRef.append', mock_append)

    def mock_label(self, km: str):
        pass
    monkeypatch.setattr(
        'prairiedog.graph_ref.GraphRef.write_graph_label', mock_label)

    KmerGraph(genome_files_shortened)
    for sg_1, sg_2 in itertools.combinations(subgraphs, 2):
        # Node IDs in every subgraph should be unique
        assert sg_1.graph.nodes.isdisjoint(sg_2.graph.nodes)

    for sg in subgraphs:
        assert sg.km.unique_kmers == len(sg.graph.nodes)
