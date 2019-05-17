import pytest
import logging
import datetime
import itertools

import prairiedog.graph_ref
from prairiedog.kmer_graph import KmerGraph

log = logging.getLogger("prairiedog")


def test_kmer_graph_benchmark(memory_profiler, all_genome_files, g):
    log.info("Benchmarking with {} genome files".format(len(all_genome_files)))

    def profile():
        kmg = KmerGraph(all_genome_files, g)
        assert isinstance(kmg, KmerGraph)
        pf = "{date:%Y-%m-%d_%H-%M-%S}_{n}-files".format(
            date=datetime.datetime.now(), n=len(all_genome_files))
        kmg.graph.save(pf)
    mem_usage = memory_profiler.memory_usage(profile)
    log.info("Memory usage was: {} MB".format(max(mem_usage)))


def test_kmer_graph_load(monkeypatch, genome_files):
    subgraphs = []

    # Monkeypatch KmerGraph's call to GraphRef.append so we collect the
    # subgraphs
    def mockappend(subgraph):
        subgraphs.append(subgraph)
    monkeypatch.setattr(prairiedog.graph_ref.GraphRef, 'append', mockappend)

    KmerGraph(genome_files)
    for sg_1, sg_2 in itertools.combinations(subgraphs, 2):
        # Node IDs in every subgraph should be unique
        assert sg_1.graph.nodes.isdisjoint(sg_2)
