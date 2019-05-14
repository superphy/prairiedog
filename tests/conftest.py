import os
import math
import shutil
import logging

import pytest

from prairiedog import kmers
from prairiedog.networkx_graph import NetworkXGraph
from prairiedog.kmer_graph import KmerGraph

log = logging.getLogger('prairiedog')

GENOME_FILES = [
    "tests/172.fa",
    "tests/ECI-2866_lcl.fasta",
    "tests/GCA_900015695.1_ED647_contigs_genomic.fna"
]


@pytest.fixture
def genome_files():
    return GENOME_FILES


@pytest.fixture(params=[.25, .5, .75, 1])
def all_genome_files(request):
    def _files(directory: str = 'samples/'):
        fls = [directory + f for f in os.listdir(directory)
               if f.endswith(('.fna', '.fasta', '.fa'))]
        return fls

    # Check if sample files exist
    if _files():
        files = _files()
    else:
        # Copy some files in
        log.warning(
            "No genomes files were found in samples/, copying from tests/")
        test_files = _files('tests/')
        for tf in test_files:
            shutil.copy2(tf, 'samples/')
        files = _files()
    # Round up to nearest int
    n = math.ceil(len(files) * request.param)
    return files[: n]


@pytest.fixture(scope="function", params=GENOME_FILES)
def km(request):
    return kmers.Kmers(request.param)


# TODO: use params to test against multiple backing stores
@pytest.fixture(scope="function", params=["networkx"])
def g(request):
    if request.param == "networkx":
        return NetworkXGraph()


@pytest.fixture
def memory_profiler():
    """
    Helper function to run a pytest if the memory_profiler PyPI package is
    installed.
    :return:
    """
    memory_profiler = pytest.importorskip("memory_profiler")
    return memory_profiler