import tempfile
import pathlib
import logging
import glob

import pytest

from prairiedog.node import Node, DEFAULT_NODE_TYPE, concat_values
from prairiedog.dgraph import DEFAULT_EDGE_PREDICATE
from prairiedog.dgraph_bundled_helper import DgraphBundledHelper


log = logging.getLogger('prairiedog')


@pytest.fixture
def dgraph_bundled_helper() -> DgraphBundledHelper:
    g = DgraphBundledHelper()
    return g


def test_dgraph_bulk_basics(dgraph_bundled_helper: DgraphBundledHelper):
    dg = dgraph_bundled_helper
    tmp_dir = tempfile.mkdtemp()
    r1 = pathlib.Path(tmp_dir, 'r1.rdf')
    with open(r1, 'a') as f:
        f.write('_:a <{n}> "ATCG" . \n_:b <{n}> "ATGC" . \n'.format(
            n=DEFAULT_NODE_TYPE))
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
        raise e


def test_dgraph_bulk_basics_path(dgraph_bundled_helper: DgraphBundledHelper):
    dg = dgraph_bundled_helper
    tmp_dir = tempfile.mkdtemp()
    r1 = pathlib.Path(tmp_dir, 'r1.rdf')
    with open(r1, 'a') as f:
        f.write('_:a <{n}> "ATCG" . \n_:b <{n}> "TCGG" . \n'.format(
            n=DEFAULT_NODE_TYPE))
        f.write('_:a <{e}> _:e . \n_:e <{e}> _:b . \n'.format(
            e=DEFAULT_EDGE_PREDICATE))
        f.write('_:e <type> "contig1" . \n')
        f.write('_:e <value> "0" . \n')
    try:
        dg.load(tmp_dir)

        connected, starting_edges = dg.g.connected('ATCG', 'TCGG')
        assert connected
        log.info("Connectivity check passed")
        assert len(starting_edges) == 1
        log.info("Length of starting edges check passed")

        paths, _ = dg.g.path('ATCG', 'TCGG')
        assert len(paths) == 1
        log.info("Length of paths check passed")
        path = paths[0]
        assert path[0].value == 'ATCG'
        assert path[1].value == 'TCGG'
        log.info("Path value checks passed")
        joined = concat_values(path)
        assert joined == 'ATCGG'
        log.info("Joined path check passed")
    except Exception as e:
        log.critical("Couldn't find one of the expected nodes")
        files = glob.glob('{}/**'.format(dg.g.out_dir), recursive=True)
        log.critical("Files is out_dir are:\n{}".format(files))
        raise e


# TODO: get this to run on CircleCI without being killed
# def test_dgraph_bulk_snakemake(dgraph_build: DgraphBundled):
#     na = Node(value='GTGCAAGTGAG')
#     nb = Node(value='AAATTCCGGAC')
#     try:
#         assert dgraph_build.exists_node(na)
#         assert dgraph_build.exists_node(nb)
#     except:
#         raise GraphException(dgraph_build)
