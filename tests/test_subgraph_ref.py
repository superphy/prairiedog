import pytest
import logging

from prairiedog.subgraph_ref import SubgraphRef
from prairiedog.graph_ref import GraphRef

log = logging.getLogger("prairiedog")


def test_subgraph_creation(g, km_short):
    # TODO: figure out a way to test LemonGraph on CircleCI in a reasonable
    #   runtime
    sgr = SubgraphRef(g)
    gr = GraphRef()

    sgr.update_graph(km_short, gr)

    assert True
