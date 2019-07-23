#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `prairiedog` package."""

import pytest

from prairiedog.graph import Graph
from prairiedog.node import Node
from prairiedog.edge import Edge
from prairiedog.errors import GraphException
from prairiedog.lemon_graph import LGGraph
from prairiedog.dgraph import Dgraph


def test_graph_basics_nodes(g: Graph):
    expected = ["ABC", "BCE", "CEF"]
    expected = set(Node(value=v) for v in expected)
    for node in expected:
        g.upsert_node(node)
    # We assume the backing store might change ordering and only care for the
    # node values
    assert set(n.value for n in g.nodes) == set(ne.value for ne in expected)


# TODO: merge lemongraph and graph basic edges tests
def test_graph_basics_edges_lemongraph(lgr: LGGraph):
    g = lgr
    expected = ["ABC", "BCE", "CEF"]
    expected = [Node(value=v) for v in expected]

    nodes_with_ids = []
    for node in expected:
        # Returned the node with db_id set, these are required to check edges
        n = g.upsert_node(node)
        nodes_with_ids.append(n)

    g.add_edge(Edge(src=expected[0].value, tgt=expected[1].value))
    g.add_edge(Edge(src=expected[1].value, tgt=expected[2].value))
    try:
        # In lemongraph these are stored as numerical IDs
        assert {(e.src, e.tgt) for e in g.edges} == {
            (nodes_with_ids[0].db_id, nodes_with_ids[1].db_id),
            (nodes_with_ids[1].db_id, nodes_with_ids[2].db_id)}
    except:
        raise GraphException(g)


def test_graph_basics_edges_dgraph(dg: Dgraph):
    g = dg
    expected = ["ABC", "BCE", "CEF"]
    expected = [Node(value=v) for v in expected]

    g.add_edge(Edge(src=expected[0].value, tgt=expected[1].value))
    g.add_edge(Edge(src=expected[1].value, tgt=expected[2].value))
    try:
        # In Dgraph we can retrieve the actual kmer value
        assert {(e.src, e.tgt) for e in g.edges} == {
            (expected[0].value, expected[1].value),
            (expected[1].value, expected[2].value)}
    except:
        raise GraphException(g)


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
