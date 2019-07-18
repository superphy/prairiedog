import logging
import typing
import time
import pathlib
import json

import pydgraph
import grpc

from prairiedog.edge import Edge
from prairiedog.graph import Graph
from prairiedog.node import Node
from prairiedog.kmers import possible_kmers

log = logging.getLogger("prairiedog")

DGRAPH_URL = 'localhost:9080'
SCHEME = ''


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

    def exists_node(self, node: Node) -> bool:
        query = """{{
            q(func: eq({predicate}, "{value}")) {{
                    expand(_all_)
                    }}
            }}
            """.format(predicate=node.node_type, value=node.value)
        log.debug("Using query: \n{}".format(query))
        res = self.client.txn(read_only=True).query(query)
        if type(res) is bytes:
            res = str(res)
        r = json.loads(res.json)
        if len(r['q'] == 0):
            return False
        else:
            return True

    def upsert_node(self, node: Node, echo: bool = True) -> typing.Optional[
            Node]:
        if self.exists_node(node):
            return
        else:
            pass

    def add_edge(self, edge: Edge, echo: bool = True) -> typing.Optional[Edge]:
        self.nquads += """
        _:{src} <fd> _:{tgt} (type="{type}", value={value}) .
        """.format(src=edge.src, tgt=edge.tgt, type=edge.edge_type,
                   value=edge.edge_value)

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
