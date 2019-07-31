# -*- coding: utf-8 -*-

"""Console script for prairiedog."""
import click
import os

from prairiedog.logger import setup_logging
from prairiedog.prairiedog import Prairiedog
from prairiedog.graph import Graph
from prairiedog.lemon_graph import LGGraph, DB_PATH
from prairiedog.dgraph import Dgraph

# If cli is imported, re-setup logging to level INFO
setup_logging("INFO")


def connect_lemongraph() -> LGGraph:
    # If the path doesn't exist (ie. a tempfile for tests), LemonGraph will
    # error out if you set readonly
    if os.path.exists(DB_PATH):
        g = LGGraph(readonly=True)
    else:
        g = LGGraph(readonly=False)
    return g


def parse_backend(backend: str) -> Graph:
    if backend == 'dgraph':
        g = Dgraph()
    elif backend == 'lemongraph':
        g = connect_lemongraph()
    else:
        g = Dgraph()
    return g


@click.command()
@click.argument('src', nargs=1)
@click.argument('dst', nargs=1)
@click.option('--backend', default='dgraph', help='Backend graph database')
def query(src: str, dst: str, backend: str):
    g = parse_backend(backend)
    pdg = Prairiedog(g=g)
    pdg.query(src, dst)
