#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `prairiedog` package."""

import pytest

import logging

log = logging.getLogger('prairiedog')


def test_logs():
    """Checks that logs are being recorded correctly.
    """
    msg = "Test"
    expected = "DEBUG - {}".format(msg)
    log.debug(msg)
    found = False
    with open("prairiedog.log") as f:
        lines = [l.rstrip() for l in f.readlines()]
        print(lines)
        for line in lines:
            if expected in line:
                found = True
                break
    assert found


