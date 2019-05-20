# -*- coding: utf-8 -*-

"""Main module."""

import prairiedog.config as config
from prairiedog.kmer_graph import KmerGraph


class Prairiedog:
    def __init__(self):
        self.run()

    def run(self):
        KmerGraph(config.INPUT_FILES, config.K)
