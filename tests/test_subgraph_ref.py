import pytest
import logging

from prairiedog.profiler import Profiler
from prairiedog.subgraph_ref import SubgraphRef
from prairiedog.graph_ref import GraphRef

log = logging.getLogger("prairiedog")


def _do_subgraph(g, km):
    sgr = SubgraphRef(g)
    gr = GraphRef()

    sgr.update_graph(km, gr)

    assert True


def test_subgraph_creation(g, km_short):
    _do_subgraph(g, km_short)


def test_subgraph_creation_profile(g, km_short):
    profiler = Profiler()
    profiler.start()

    _do_subgraph(g, km_short)

    profiler.stop()

    print(profiler.output_text(unicode=True, color=True))
