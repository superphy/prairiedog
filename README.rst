==========
prairiedog
==========

.. image:: .daisy.png

Supports Python3.5+

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
