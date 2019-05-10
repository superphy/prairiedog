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
    log.debug(msg)
    found = False
    with open("prairiedog.log") as f:
        for li in f:
            line = li.rstrip()
            if msg in line:
                found = True
                break
    assert found


