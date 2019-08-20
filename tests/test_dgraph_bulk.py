import tempfile
import pathlib
import logging
import time
import os
import shutil
import glob

import pytest

from prairiedog.node import Node
from prairiedog.dgraph_bundled import DgraphBundled
from prairiedog.errors import DgraphBundledException
from dgraph.bulk import run_dgraph_bulk


log = logging.getLogger('prairiedog')


class DgraphBundledHelper:
    """For loading arbitrary rdf and testing"""
    def __init__(self):
        self.tmp_output = tempfile.mkdtemp()
        self.tmp_samples = tempfile.mkdtemp()
        self._g = None

    def load(self, rdf_dir: str, delete_after: bool = True) -> pathlib.Path:
        log.info("Loading rdf from {} ...".format(rdf_dir))
        for f in os.listdir(rdf_dir):
            fp = pathlib.Path(rdf_dir, f)
            log.info("Will load {} ...".format(fp))
            shutil.copy2(fp, self.tmp_samples)
        p = pathlib.Path(self.tmp_output, 'dgraph')
        self._g = DgraphBundled(delete=False, output_folder=p)
        run_dgraph_bulk(cwd=p, move_to=self.g.postings_dir,
                        rdfs=self.tmp_samples, zero_port=self.g.zero_port)
        # Reinitialize DgraphBundled after moving postings files
        self._g.shutdown_dgraph()
        del self._g
        self._g = DgraphBundled(delete=delete_after, output_folder=p)
        return p

    @property
    def g(self) -> DgraphBundled:
        if self._g is not None:
            return self._g
        else:
            msg = "load() must be called before accessing g"
            log.critical(msg)
            raise AttributeError(msg)


@pytest.fixture
def dgraph_bundled_helper() -> DgraphBundledHelper:
    g = DgraphBundledHelper()
    return g


def test_dgraph_bulk_basics(dgraph_bundled_helper: DgraphBundledHelper):
    dg = dgraph_bundled_helper
    tmp_dir = tempfile.mkdtemp()
    r1 = pathlib.Path(tmp_dir, 'r1.rdf')
    with open(r1, 'a') as f:
        f.write('_:a <km> "ATCG" . \n')
        f.write('_:b <km> "ATGC" . \n')
    try:
        dg.load(tmp_dir)
        exists, _ = dg.g.exists_node(Node(value="ATCG"))
        assert exists
        log.info("ATCG exists")
        exists, _ = dg.g.exists_node(Node(value="ATGC"))
        assert exists
        log.info("ATGC exists")
    except Exception as e:
        log.critical("Couldn't find one of the expected nodes")
        files = glob.glob('{}/**'.format(dg.g.out_dir), recursive=True)
        log.critical("Files is out_dir are:\n{}".format(files))
        raise DgraphBundledException(dg.g)


# TODO: get this to run on CircleCI without being killed
# def test_dgraph_bulk_snakemake(dgraph_build: DgraphBundled):
#     na = Node(value='GTGCAAGTGAG')
#     nb = Node(value='AAATTCCGGAC')
#     try:
#         assert dgraph_build.exists_node(na)
#         assert dgraph_build.exists_node(nb)
#     except:
#         raise GraphException(dgraph_build)
