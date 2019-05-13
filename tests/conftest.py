import pytest

from prairiedog import kmers
from prairiedog.networkx_graph import NetworkXGraph
from prairiedog.kmer_graph import KmerGraph

GENOME_FILES = [
    "tests/172.fa",
    "tests/ECI-2866_lcl.fasta",
    "tests/GCA_900015695.1_ED647_contigs_genomic.fna"
]


@pytest.fixture
def genome_files():
    return GENOME_FILES


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
