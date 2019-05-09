#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `prairiedog` package."""

import pytest

from prairiedog import kmers


@pytest.fixture
def example_kmers(f="tests/172.fa"):
    km = kmers.Kmers(f)
    return km


def test_kmers_load():
    km = example_kmers()
    e = ">gi|1062504329|gb|CP014670.1| Escherichia coli strain CFSAN004177, complete genome"
    assert km.headers[0] == e


def test_kmers_next():
    km = example_kmers()
    header, kmer = km.next()
    assert header == ">gi|1062504329|gb|CP014670.1| Escherichia coli strain CFSAN004177, complete genome"
    assert kmer == "TCGCTTTCGTT"
    header, kmer = km.next()
    assert header == ">gi|1062504329|gb|CP014670.1| Escherichia coli strain CFSAN004177, complete genome"
    assert kmer == "CGCTTTCGTTC"
