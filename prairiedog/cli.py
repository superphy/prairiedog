# -*- coding: utf-8 -*-

"""Console script for prairiedog."""
import click
import os
import subprocess

import psutil

from prairiedog.logger import setup_logging
from prairiedog.prairiedog import Prairiedog
from prairiedog.graph import Graph
from prairiedog.lemon_graph import LGGraph, DB_PATH
from prairiedog.dgraph_bundled import DgraphBundled
from prairiedog.kmers import recommended_procs_kmers

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
        g = DgraphBundled()
    elif backend == 'lemongraph':
        g = connect_lemongraph()
    else:
        g = DgraphBundled(delete=False, output_folder='outputs/dgraph/')
    return g


@click.group()
@click.option('--debug/--no-debug', default=False)
def cli(debug):
    click.echo('Debug mode is %s' % ('on' if debug else 'off'))
    if debug:
        setup_logging('DEBUG')


@cli.command()
@click.argument('src', nargs=1)
@click.argument('dst', nargs=1)
@click.option('--backend', default='dgraph', help='Backend graph database')
def query(src: str, dst: str, backend: str):
    """Query the pan-genome for a path between two k-mers."""
    g = parse_backend(backend)
    pdg = Prairiedog(g=g)
    pdg.query(src, dst)


@cli.command()
def dgraph():
    """Create a pan-genome."""
    subprocess.run("snakemake -j {} dgraph".format(
        recommended_procs_kmers), shell=True)
