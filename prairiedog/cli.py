# -*- coding: utf-8 -*-

"""Console script for prairiedog."""
import click

from prairiedog.logger import setup_logging
from prairiedog.prairiedog import Prairiedog
from prairiedog.lemon_graph import LGGraph

# If cli is imported, re-setup logging to level INFO
setup_logging("INFO")

pdg = Prairiedog(g=LGGraph())


@click.command()
@click.argument('src', nargs=1)
@click.argument('dst', nargs=1)
def query(src: str, dst: str):
    pdg.query(src, dst)
