import logging
import typing
import time
import pathlib
import json
import sys

import pydgraph
import grpc

from prairiedog.edge import Edge
from prairiedog.graph import Graph
from prairiedog.node import Node, DEFAULT_NODE_TYPE
from prairiedog.kmers import possible_kmers

log = logging.getLogger("prairiedog")

DGRAPH_URL = 'localhost:9080'
SCHEME = ''

# This is specific to Dgraph
DEFAULT_EDGE_PREDICATE = 'fd'


def decode(b: bytes):
    if sys.version_info < (3, 6):
        return json.loads(b.decode('utf-8'))
    else:
        return json.loads(b)


class Dgraph(Graph):

    def __init__(self):
        self._client_stub = None
        self._client = None
        self.nquads = ""
        log.debug("Done initializing Dgraph client")

    @property
    def client_stub(self):
        if self._client_stub is None:
            self._client_stub = pydgraph.DgraphClientStub(DGRAPH_URL)
        return self._client_stub

    @property
    def client(self):
        if self._client is None:
            self._client = pydgraph.DgraphClient(self.client_stub)
        return self._client

    def __del__(self):
        if self._client_stub is not None:
            self.client_stub.close()

    def preload(self, k: int = 11):
        nquads = ""
        c = 0
        for kmer in possible_kmers(k):
            nquads += ' _:{kmer} <km> "{kmer}" .'.format(kmer=kmer)
            c += 1
            if c % 333 == 0:
                self.mutate(nquads)
                nquads = ""
        self.mutate(nquads)

    def query(self, q: str):
        log.debug("Using query: \n{}".format(q))
        res = self.client.txn(read_only=True).query(q)
        log.debug("Got res: \n{}\n of type {}".format(res, type(res)))
        r = decode(res.json)
        log.debug("Decoded as: \n{}".format(r))
        return r

    @staticmethod
    def _exists(r) -> typing.Tuple[bool, str]:
        if len(r['q']) != 0:
            assert len(r['q']) == 1
            return True, r['q'][0]['uid']
        else:
            return False, ""

    def exists_node(self, node: Node) -> typing.Tuple[bool, str]:
        query = """{{
            q(func: eq({predicate}, "{value}")) {{
                    uid
                    }}
            }}
            """.format(predicate=node.node_type, value=node.value)
        r = self.query(query)
        return Dgraph._exists(r)

    def upsert_node(self, node: Node, echo: bool = True) -> typing.Optional[
            Node]:
        exists, uid = self.exists_node(node)
        if exists:
            if echo:
                return Node(node_type=node.node_type, value=node.value,
                            db_id=uid)
            else:
                return
        else:
            nquads = '_:{value} <{type}> "{value}" .'.format(
                value=node.value, type=node.node_type)
            self.mutate(nquads)
            if echo:
                return self.upsert_node(node)
            else:
                return

    def exists_edge(self, edge: Edge, node_type: str = None,
                    edge_predicate: str = None) -> typing.Tuple[bool, str]:
        if node_type is None:
            node_type = DEFAULT_NODE_TYPE
        if edge_predicate is None:
            edge_predicate = DEFAULT_EDGE_PREDICATE
        query = """{{
            q(func: eq({node_type}, "{src}")) {{
                {edge_predicate} @facets(
                    eq(type, {facet_type}) AND eq(value, {facet_value})
                ) @filter(eq({node_type}, {tgt})) {{
                    uid
                }}
            }}
        }}
        """.format(node_type=node_type, src=edge.src,
                   edge_predicate=edge_predicate, facet_type=edge.edge_type,
                   facet_value=edge.edge_value, tgt=edge.tgt)
        r = self.query(query)
        return Dgraph._exists(r)

    def upsert_edge(self, edge: Edge, node_type: str = None,
                    edge_predicate: str = None):
        if node_type is None:
            node_type = DEFAULT_NODE_TYPE
        if edge_predicate is None:
            edge_predicate = DEFAULT_EDGE_PREDICATE
        exists, uid = self.exists_edge(edge, node_type=node_type,
                                       edge_predicate=edge_predicate)
        if exists:
            return
        else:
            nquads = """
            _:a <{node_type}> "{src}" .
            _:b <{node_type}> "{tgt}" .
            _:a <{edge_predicate}> _:b (type={edge_type}, value={edge_value}) .
            """.format(node_type=node_type, src=edge.src, tgt=edge.tgt,
                       edge_predicate=edge_predicate, edge_type=edge.edge_type,
                       edge_value=edge.edge_value)
            self.mutate(nquads)

    def add_edge(self, edge: Edge, echo: bool = True) -> typing.Optional[Edge]:
        self.nquads += """
        _:{src} <{edge_type}> _:{tgt} ({facet_label}={facet_value}) .
        """.format(src=edge.src, tgt=edge.tgt, facet_label=edge.edge_type,
                   facet_value=edge.edge_value,
                   edge_type=DEFAULT_EDGE_PREDICATE)

    def clear(self):
        op = pydgraph.Operation(drop_all=True)
        self.client.alter(op)

    @property
    def nodes(self) -> typing.Set[Node]:
        pass

    @property
    def edges(self) -> typing.Set[Edge]:
        pass

    def get_labels(self, node: str) -> dict:
        pass

    def mutate(self, nquads: str, depth: int = 1, max_depth: int = 3):
        txn = self.client.txn()
        try:
            if depth > 1:
                log.debug("Trying mutation attempt {}/{}".format(depth,
                                                                 max_depth))
            txn.mutate(set_nquads=nquads)
            txn.commit()
            # self._txn = None
            if depth > 1:
                log.debug("Attempt {}/{} completed successfully".format(
                    depth, max_depth
                ))
        except grpc.RpcError as rpc_error_call:
            if depth <= max_depth:
                log.debug("Ran into exception {}, retrying {}/{}...".format(
                    rpc_error_call, depth, max_depth
                ))
                time.sleep(2**depth)
                self.mutate(nquads=nquads, depth=depth+1)
        except Exception as e:
            log.debug("Exception type was {}".format(type(e)))
            raise e
        finally:
            # Only discard at root
            # if depth == 1:
            txn.discard()
            # self._txn = None

    def save(self, f: str = None):
        # self.mutate(self.nquads)
        p = pathlib.Path(f)
        # Make the subdirectories if required
        # str() cast is required for Py3.5
        pathlib.Path(str(p.parent)).mkdir(parents=True, exist_ok=True)
        with open(str(p), 'a+') as out_file:
            out_file.write(self.nquads)
            if not self.nquads.endswith('\n'):
                out_file.write('\n')
        self.nquads = ''

    @property
    def edgelist(self) -> typing.Generator:
        pass

    def set_graph_labels(self, labels: dict):
        pass

    def filter(self):
        pass

    def __len__(self):
        pass

    def connected(self, node_a: str, node_b: str) -> typing.Tuple[
            bool, typing.Tuple]:
        pass

    def path(self, node_a: str, node_b: str) -> tuple:
        pass
