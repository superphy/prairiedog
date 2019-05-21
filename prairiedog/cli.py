# -*- coding: utf-8 -*-

"""Console script for prairiedog."""
import sys
import os
import logging

import click

import prairiedog.config as config
from prairiedog.prairiedog import Prairiedog

log = logging.getLogger("prairiedog")


@click.command()
@click.option('-k', default=11, help='K-mer size.')
@click.option('-i', default='samples/',
              help='Folder of file to input')
def main(k, i):
    """Console script for prairiedog."""
    # Setup config
    config.K = k
    if os.path.isdir(i):
        config.INPUT_DIRECTORY = i
    else:
        config.INPUT_FILES = [i]
    # Main call
    if len(config.INPUT_FILES) == 0:
        log.critical("No input files found in folder {}, exiting...".format(
            config.INPUT_DIRECTORY))
        return
    else:
        Prairiedog()
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
