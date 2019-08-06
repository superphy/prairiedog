==========
prairiedog
==========

.. image:: .daisy.png

.. image:: https://circleci.com/gh/superphy/prairiedog.svg?style=svg
    :target: https://circleci.com/gh/superphy/prairiedog

.. image:: https://codecov.io/gh/superphy/prairiedog/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/superphy/prairiedog

Supports Python3.5+ on Linux

============
Installation
============

We recommend you follow both the installation step for graph creation
and for querying the graph, unless you are computing the graph in one
place, and querying it in another.

Both steps require you to first install lemongraph.

Clone prairiedog and install lemongraph
---------------------------------------

::

    git clone --recursive https://github.com/superphy/prairiedog.git
    cd prairiedog/
    python3 -m venv venv
    . venv/bin/activate
    cd lemongraph/
    apt-get install libffi-dev zlib1g-dev python-dev python-cffi
    python setup.py install

For creating a graph
--------------------

::

    . venv/bin/activate
    pip install -r requirements.txt
    pip install git+https://github.com/pytries/datrie.git
    pip install snakemake

For querying an existing graph
------------------------------

::

    . venv/bin/activate
    python setup.py install

=====
Usage
=====

Docker
------

::

    docker run -v /abs-path-to/outputs/:/pdg/outputs/ -v /abs-path-to/samples/:/pdg/samples/ superphy/prairiedog dgraph

For creating a graph
---------------------

::

    . venv/bin/activate
    snakemake -j 24 --config samples=samples/

For querying an existing graph
-------------------------------

::

    . venv/bin/activate
    prairiedog ATACGACGCCA CGTCCGGACGT

You should get something like:

::

    prairiedog GGGCGTTAAGT GGCAGGTTGAA
    prairiedog[21238] INFO Looking for all strings between GGGCGTTAAGT and GGCAGGTTGAA ...
    prairiedog[21238] INFO Found {'string': 'GGGCGTTAAGTTGCAGGGTATAGACCCGAAACCCGGTGATCTAGCCATGGGCAGGTTGAA', 'edge_type': 'SRR3295769.fasta', 'edge_value': '>SRR3295769.fasta|NODE_75_length_556_cov_349.837_ID_5290_pilon'}
    prairiedog[21238] INFO Found {'string': 'GGGCGTTAAGTTGCAGGGTATAGACCCGAAACCCGGTGATCTAGCCATGGGCAGGTTGAA', 'edge_type': 'SRR3665189.fasta', 'edge_value': '>SRR3665189.fasta|NODE_60_length_523_cov_287.621_ID_4672'}

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
