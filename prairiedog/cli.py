# -*- coding: utf-8 -*-

"""Console script for prairiedog."""
import click
import os

from prairiedog.logger import setup_logging
from prairiedog.prairiedog import Prairiedog
from prairiedog.lemon_graph import LGGraph, DB_PATH

# If cli is imported, re-setup logging to level INFO
setup_logging("INFO")

# If the path doesn't exist (ie. a tempfile for tests), LemonGraph will
# error out if you set readonly
if os.path.exists(DB_PATH):
    pdg = Prairiedog(g=LGGraph(readonly=True))
else:
    pdg = Prairiedog(g=LGGraph(readonly=False))


@click.command()
@click.argument('src', nargs=1)
@click.argument('dst', nargs=1)
def query(src: str, dst: str):
    pdg.query(src, dst)
