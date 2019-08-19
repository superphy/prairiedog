import tempfile
import pathlib
import logging

from prairiedog.node import Node
from prairiedog.dgraph_bundled import DgraphBundled
from prairiedog.errors import GraphException


log = logging.getLogger('prairiedog')


def test_dgraph_bulk_basics(dgraph_bundled_helper):
    dg = dgraph_bundled_helper
    tmp_dir = tempfile.mkdtemp()
    r1 = pathlib.Path(tmp_dir, 'r1.rdf')
    with open(r1, 'a') as f:
        f.write('_:a <km> "ATCG" . \n')
        f.write('_:b <km> "ATGC" . \n')
    dg.load(tmp_dir)
    assert dg.g.exists_node(Node(value="ATCG"))
    log.info("ATCG exists")
    assert dg.g.exists_node(Node(value="ATGC"))
    log.info("ATGC exists")

# TODO: get this to run on CircleCI without being killed
# def test_dgraph_bulk_snakemake(dgraph_build: DgraphBundled):
#     na = Node(value='GTGCAAGTGAG')
#     nb = Node(value='AAATTCCGGAC')
#     try:
#         assert dgraph_build.exists_node(na)
#         assert dgraph_build.exists_node(nb)
#     except:
#         raise GraphException(dgraph_build)
