import os
import math
import shutil
import logging
import itertools
import subprocess
import tempfile

import pytest

from prairiedog import kmers
from prairiedog.graph import Graph
from prairiedog.networkx_graph import NetworkXGraph
from prairiedog.lemon_graph import LGGraph
from prairiedog.dgraph_bundled import DgraphBundled
from prairiedog.cli import run_dgraph_snakemake

log = logging.getLogger('prairiedog')

#########
# Globals
#########

GENOME_FILES = [
    "tests/172.fa",
    "tests/ECI-2866_lcl.fasta",
    "tests/GCA_900015695.1_ED647_contigs_genomic.fna"
]


GENOME_FILES_SHORTENED = [
    "tests/GCA_900015695.1_ED647_contigs_genomic_SHORTENED.fasta",
    "tests/SRR1060582_SHORTENED.fasta",
    "tests/SRR1106609_SHORTENED.fasta",
]

# TODO: switch to just one backend
BACKENDS = ['dgraph', 'lemongraph']

#########
# Files
#########


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

#########
# Kmers
#########


@pytest.fixture(scope="function", params=GENOME_FILES)
def km(request):
    return kmers.Kmers(request.param)


@pytest.fixture(scope="function", params=GENOME_FILES_SHORTENED)
def km_short(request):
    return kmers.Kmers(request.param)

#########
# Graphs
#########


def _lgr():
    fd, path = tempfile.mkstemp()
    os.close(fd)
    return LGGraph(path, delete_on_exit=True)


def _dg():
    return DgraphBundled()


@pytest.fixture
def dg():
    return _dg()


@pytest.fixture()
def lgr():
    return _lgr()

# TODO: use params to test against multiple backing stores
@pytest.fixture(scope="function", params=BACKENDS)
def g(request):
    if request.param == "networkx":
        return NetworkXGraph()
    elif request.param == "lemongraph":
        # Create a new LemonGraph instance with its own database file
        return _lgr()
    elif request.param == "dgraph":
        return _dg()


@pytest.fixture
def dgraph_build() -> DgraphBundled:
    tmp_output = tempfile.mkdtemp()
    tmp_samples = tempfile.mkdtemp()
    for f in GENOME_FILES_SHORTENED:
        shutil.copy2(f, tmp_samples)
    run_dgraph_snakemake(
        '--config outputs={outputs} --config samples={samples}'.format(
            outputs=tmp_output, samples=tmp_samples
        ))
    p = os.path.join(tmp_output, 'dgraph/')
    g = DgraphBundled(delete=True, output_folder=p)
    return g

#########
# Prebuilt Graphs
#########


def lg_prebuilt() -> LGGraph:
    """
    A pre-made LemonGraph made from:
        - SRR1060582_SHORTENED.fasta,
        - SRR1106609_SHORTENED.fasta
        - GCA_900015695.1_ED647_contigs_genomic_SHORTENED.fasta
    :return:
    """
    g = LGGraph('tests/pangenome.lemongraph', delete_on_exit=False)
    return g


def dgraph_prebuilt() -> DgraphBundled:
    pass


@pytest.fixture(params=['lemongraph'])
def prebuilt_graph(request) -> Graph:
    if request.param == 'lemongraph':
        return lg_prebuilt()
    elif request.param == 'dgraph':
        return dgraph_prebuilt()

#########
# Misc
#########


# TODO: unused
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
