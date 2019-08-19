import tempfile
import pathlib

from prairiedog.node import Node
from prairiedog.dgraph_bundled import DgraphBundled
from prairiedog.errors import GraphException


def test_dgraph_bulk_basics(dgraph_bundled_helper):
    dg = dgraph_bundled_helper
    tmp_dir = tempfile.mkdtemp()
    r1 = pathlib.Path(tmp_dir, 'r1.rdf')
    with open(r1, 'a') as f:
        f.write('_:a <km> "ATCG" . ')
        f.write('_:b <km> "ATGC" . ')
    dg.load(tmp_dir)
    assert dg.g.exists_node(Node(value="ATCG"))
    assert dg.g.exists_node(Node(value="ATGC"))

# TODO: get this to run on CircleCI without being killed
# def test_dgraph_bulk_snakemake(dgraph_build: DgraphBundled):
#     na = Node(value='GTGCAAGTGAG')
#     nb = Node(value='AAATTCCGGAC')
#     try:
#         assert dgraph_build.exists_node(na)
#         assert dgraph_build.exists_node(nb)
#     except:
#         raise GraphException(dgraph_build)
