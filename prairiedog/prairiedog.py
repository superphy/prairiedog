# -*- coding: utf-8 -*-

"""Main module."""
import logging

from prairiedog.graph import Graph
from prairiedog.node import concat_values

log = logging.getLogger("prairiedog")


class Prairiedog:
    def __init__(self, g: Graph):
        self.g = g

    def query(self, src: str, dst: str):
        log.info("Looking for all strings between {} and {} ...".format(
            src, dst))
        paths, paths_meta = self.g.path(src, dst)
        list_hits = []
        for i in range(len(paths)):
            path = paths[i]
            meta = paths_meta[i]
            string = concat_values(path)
            list_hits.append(
                {
                    'string': string,
                    **meta
                }
            )
        for hit in list_hits:
            log.info("Found {}".format(hit))
