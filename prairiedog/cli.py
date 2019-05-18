# -*- coding: utf-8 -*-

"""Console script for prairiedog."""
import sys
import os

import click

import prairiedog.config as config
from prairiedog.prairiedog import Prairiedog


@click.command()
@click.option('-k', default=11, help='K-mer size.')
@click.option('--input_folder', default='samples/',
              help='Folder of file to input')
def main(k, input_folder):
    """Console script for prairiedog."""
    # Setup config
    config.K = k
    if os.path.isdir(input_folder):
        config.INPUT_DIRECTORY = input_folder
    else:
        config.INPUT_FILES = [input_folder]
    # Main call
    pdog = Prairiedog()
    pdog.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
