#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `prairiedog` package."""

import pytest

from prairiedog.graph import Graph
from prairiedog.node import Node
from prairiedog.edge import Edge


def test_graph_basics_nodes(g: Graph):
    expected = ["ABC", "BCE", "CEF"]
    expected = set(Node(value=v) for v in expected)
    for node in expected:
        g.upsert_node(node)
    # We assume the backing store might change ordering and only care for the
    # node values
    assert set(n.value for n in g.nodes) == set(ne.value for ne in expected)


def test_graph_basics_edges(g: Graph):
    expected = ["ABC", "BCE", "CEF"]
    expected = [Node(value=v) for v in expected]
    g.add_edge(expected[0], expected[1])
    g.add_edge(expected[1], expected[2])
    assert g.edges == {
        (expected[0], expected[1]),
        (expected[1], expected[2])
    }


def test_graph_node_labels(g: Graph):
    expected = [
        Node(value="ABC", labels={"species": "dog"}),
        Node(value="BCE", labels={"species": "cat"})
    ]
    for node in expected:
        g.upsert_node(node)

    assert len(g.nodes) == 2

    for node in g.nodes:
        if node.value == "ABC":
            assert node.labels == expected[0].labels
        elif node.value == "BCE":
            assert node.labels == expected[1].labels
        else:
            # Something went wrong
            assert False
