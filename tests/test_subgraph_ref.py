import pytest
import logging

from prairiedog.subgraph_ref import SubgraphRef

log = logging.getLogger("prairiedog")


def test_subgraph_creation(g):
    # TODO: figure out a way to test LemonGraph on CircleCI in a reasonable
    # runtime
    sgr = SubgraphRef(g)
    assert True
