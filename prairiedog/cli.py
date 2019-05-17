# -*- coding: utf-8 -*-

"""Console script for prairiedog."""
import sys
import click

import prairiedog.config as config
from prairiedog.prairiedog import Config, Prairiedog


@click.command()
@click.option('-k', default=11, help='K-mer size.')
@click.option('--input')
def main(k):
    """Console script for prairiedog."""
    # Setup config
    config.K = k
    config.INPUT_FILES =
    # Main call
    pdog = Prairiedog()
    pdog.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
