#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `prairiedog` package."""

import pytest
import logging

import prairiedog.graph
import prairiedog.networkx_graph

log = logging.getLogger('prairiedog')

# TODO: use params to test against multiple backing stores
@pytest.fixture(scope="module", params=["networkx"])
def g(backing_store):
    if backing_store.param == "networkx":
        return prairiedog.networkx_graph.NetworkXGraph()


def test_graph_basics(g: prairiedog.graph.Graph):
    expected = ["ABC", "BCE", "CEF"]
    for node in expected:
        g.upsert_node(expected)
    assert g.nodes == expected

