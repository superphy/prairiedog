==========
prairiedog
==========

.. image:: .daisy.png

.. image:: https://circleci.com/gh/superphy/prairiedog.svg?style=svg
    :target: https://circleci.com/gh/superphy/prairiedog

.. image:: https://codecov.io/gh/superphy/prairiedog/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/superphy/prairiedog

Supports Python3.5+

Usage
-----

To recreate the models, we have to install datrie from src (see https://github.com/pytries/datrie/issues/52), run:

::

    python -m venv venv
    . venv/bin/activate
    pip install -r requirements.txt
    pip install git+https://github.com/pytries/datrie.git
    pip install snakemake
    snakemake --config samples=samples/

To use prairiedog on existing models, run:

::

    python setup.py install
    prairiedog

Tests & Benchmarks
------------------

Test genomes are included in the *tests/* folders, while genomes for
benchmarking should be included in the *samples/* folder. To run tests and
benchmarks:

::

    python3 -m venv venv
    . venv/bin/activate
    pip install tox
    tox -v
