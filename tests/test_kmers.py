#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `prairiedog` package."""

import pytest

from prairiedog import kmers


def test_kmers_load():
    km = kmers.Kmers("tests/172.fa")
    assert(km.headers[0] == ">gi|1062504329|gb|CP014670.1| Escherichia coli strain CFSAN004177, complete genome")
