import os
import math
import shutil
import logging
import itertools
import subprocess
import tempfile

import pytest

from prairiedog import kmers
from prairiedog.networkx_graph import NetworkXGraph
from prairiedog.lemon_graph import LGGraph

log = logging.getLogger('prairiedog')

GENOME_FILES = [
    "tests/172.fa",
    "tests/ECI-2866_lcl.fasta",
    "tests/GCA_900015695.1_ED647_contigs_genomic.fna"
]


@pytest.fixture
def genome_files():
    return GENOME_FILES


@pytest.fixture
def genome_files_shortened():
    fls = ['tests/' + f for f in os.listdir('tests/')
           if f.endswith(('.fna', '.fasta', '.fa'))
           and 'SHORTENED' in f
           ]
    return fls


@pytest.fixture(params=[0.001, .25, .5, .75, 1])
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


def _lgr():
    fd, path = tempfile.mkstemp()
    os.close(fd)
    return LGGraph(path, delete_on_exit=True)


# TODO: use params to test against multiple backing stores
@pytest.fixture(scope="function", params=["lemongraph"])
def g(request):
    if request.param == "networkx":
        return NetworkXGraph()
    elif request.param == "lemongraph":
        # Create a new LemonGraph instance with its own database file
        return _lgr()


@pytest.fixture(scope="function")
def lgr():
    return _lgr()


@pytest.fixture
def memory_profiler():
    """
    Helper function to run a pytest if the memory_profiler PyPI package is
    installed.
    :return:
    """
    memory_profiler = pytest.importorskip("memory_profiler")
    return memory_profiler


@pytest.fixture
def kmer_map():
    """
    Creates a fixed reference for all possible kmers to a node_id.
    :return:
    """
    mp = map(''.join, itertools.product('ATCG', repeat=11))
    d = {x: i + 1 for i, x in enumerate(mp)}
    return d


@pytest.fixture(params=[
    ['tests/SRR1060582_SHORTENED.fasta', 'tests/SRR1106609_SHORTENED.fasta']])
def setup_snakefile(request):
    for f in request.param:
        shutil.copy2(f, 'samples/')
    yield request.param
    files = [os.path.basename(f) for f in request.param]
    for f in files:
        os.remove(os.path.join('samples/', f))
    subprocess.run("snakemake clean", shell=True)


@pytest.fixture
def lg() -> LGGraph:
    """
    A pre-made LemonGraph made from:
        - SRR1060582_SHORTENED.fasta,
        - SRR1106609_SHORTENED.fasta
        - GCA_900015695.1_ED647_contigs_genomic_SHORTENED.fasta
    :return:
    """
    g = LGGraph('tests/pangenome.lemongraph', delete_on_exit=False)
    return g
