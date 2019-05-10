#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `prairiedog` package."""

import pytest
import logging

from prairiedog.graph import Graph
from prairiedog.networkx_graph import NetworkXGraph

log = logging.getLogger('prairiedog')


# TODO: use params to test against multiple backing stores
@pytest.fixture(scope="module", params=["networkx"])
def g(request):
    if request.param == "networkx":
        return NetworkXGraph()


def test_graph_basics(g: Graph):
    expected = ["ABC", "BCE", "CEF"]
    for node in expected:
        g.upsert_node(node)
    # We assume the backing store might change ordering.
    assert g.nodes == set(expected)

    g.add_edge(expected[0], expected[1])
    assert g.edges == {expected[0], expected[1]}
