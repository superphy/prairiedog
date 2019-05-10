#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `prairiedog` package."""

import pytest

from prairiedog.graph import Graph


def test_graph_basics(g: Graph):
    expected = ["ABC", "BCE", "CEF"]
    for node in expected:
        g.upsert_node(node)
    # We assume the backing store might change ordering.
    assert g.nodes == set(expected)

    g.add_edge(expected[0], expected[1])
    g.add_edge(expected[1], expected[2])
    assert g.edges == {
        (expected[0], expected[1]),
        (expected[1], expected[2])
    }


def test_graph_labels(g: Graph):
    expected = {
        "ABC": {"species": "dog"},
        "BCE": {"species": "cat"}
    }
    for k, v in expected.items():
        g.upsert_node(k, v)
    assert g.nodes == set(expected.keys())

    for k, v in expected.items():
        assert g.get_labels(k) == v
