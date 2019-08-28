# -*- coding: utf-8 -*-

"""Console script for prairiedog."""
import click
import os
import subprocess
import logging
import signal
import sys
import atexit

from prairiedog.logger import setup_logging
from prairiedog.prairiedog import Prairiedog
from prairiedog.graph import Graph
from prairiedog.lemon_graph import LGGraph, DB_PATH
from prairiedog.dgraph_bundled import DgraphBundled
from prairiedog.kmers import recommended_procs_kmers
from prairiedog.profiler import Profiler, profiler_stop

# If cli is imported, re-setup logging to level INFO
setup_logging("INFO")

log = logging.getLogger('prairiedog')


def connect_lemongraph() -> LGGraph:
    # If the path doesn't exist (ie. a tempfile for tests), LemonGraph will
    # error out if you set readonly
    if os.path.exists(DB_PATH):
        g = LGGraph(readonly=True)
    else:
        g = LGGraph(readonly=False)
    return g


def connect_dgraph(**kwargs) -> DgraphBundled:
    sys.setrecursionlimit(100000)
    p = 'outputs/dgraph/'
    # If the path exists, this was called after a Snakemake run.
    # Otherwise, "query" was called without computing the backend graph.
    if os.path.isdir(p):
        g = DgraphBundled(
            delete=False, output_folder=p, deploy=True, delay=60*5, **kwargs)
    else:
        g = DgraphBundled(**kwargs)
    return g


def parse_backend(backend: str) -> Graph:
    if backend == 'dgraph':
        g = connect_dgraph()
    elif backend == 'lemongraph':
        g = connect_lemongraph()
    else:
        g = connect_dgraph()
    return g


def run_dgraph_snakemake(additional_str: str = ""):
    """Helper to execute snakemake for dgraph"""
    cmd = "snakemake --config backend=dgraph {c} -j {j}  dgraph".format(
        c=additional_str, j=recommended_procs_kmers)
    log.info("Executing Snakemake with cmd:\n{}".format(cmd))
    subprocess.run(cmd, shell=True)


@click.group()
@click.option('--debug/--no-debug', default=False)
@click.option('--profiler/--no-profiler', default=False)
def cli(debug, profiler):
    click.echo('Debug mode is %s' % ('on' if debug else 'off'))
    if debug:
        setup_logging('DEBUG')

    click.echo('Profiler mode is %s' % ('on' if profiler else 'off'))
    if profiler:
        profiler = Profiler()
        profiler.start()
        atexit.register(profiler_stop, profiler)


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
    run_dgraph_snakemake()


@cli.command()
def ratel():
    g = connect_dgraph(ratel=True)
    log.debug("Initialized {}".format(g))
    # Suspend the main thread
    signal.pause()
