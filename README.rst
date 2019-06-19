==========
prairiedog
==========

.. image:: .daisy.png

.. image:: https://circleci.com/gh/superphy/prairiedog.svg?style=svg
    :target: https://circleci.com/gh/superphy/prairiedog

.. image:: https://codecov.io/gh/superphy/prairiedog/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/superphy/prairiedog

Supports Python3.5+

============
Installation
============

We recommend you follow both installation steps, unless you are computing the
graph in one place, and querying it in another.

For creating a graph
--------------------

::

    python3 -m venv venv
    . venv/bin/activate
    pip install -r requirements.txt
    pip install git+https://github.com/pytries/datrie.git
    pip install snakemake

For querying an existing graph
------------------------------

::

    python3 -m venv venv
    . venv/bin/activate
    python setup.py install

=====
Usage
=====

For creating a graph
---------------------

::

    . venv/bin/activate
    snakemake -j 24 --config samples=samples/

For querying an existing graph
-------------------------------

::

    prairiedog ATACGACGCCA CGTCCGGACGT

==================
Tests & Benchmarks
==================

Test genomes are included in the *tests/* folders, while genomes for
benchmarking should be included in the *samples/* folder. To run tests and
benchmarks:

::

    python3 -m venv venv
    . venv/bin/activate
    pip install tox
    tox -v
