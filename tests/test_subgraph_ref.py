import pytest
import logging

from prairiedog.profiler import Profiler
from prairiedog.subgraph_ref import SubgraphRef
from prairiedog.graph_ref import GraphRef

log = logging.getLogger("prairiedog")


def _do_subgraph(g, km):
    log.info("Setting up SubgraphRef...")
    sgr = SubgraphRef(g)
    log.info("Setting up GraphRef....")
    gr = GraphRef()

    log.info("Updating graph...")
    sgr.update_graph(km, gr)

    assert True


def test_subgraph_creation(g, km_short):
    _do_subgraph(g, km_short)


def test_subgraph_creation_profile(g, km_short):
    log.info("Initializing and starting profiler...")
    profiler = Profiler()
    profiler.start()

    _do_subgraph(g, km_short)

    log.info("Stopping profiler...")
    profiler.stop()

    log.info("Trying to print profiler test...")
    print(profiler.output_text(unicode=True, color=True))
