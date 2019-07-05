import logging
import typing
import time

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
        self.client_stub = pydgraph.DgraphClientStub(DGRAPH_URL)
        self.client = pydgraph.DgraphClient(self.client_stub)
        self.nquads = ""
        log.debug("Done initializing Dgraph client")

    def __del__(self):
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

    def upsert_node(self, node: Node, echo: bool = True) -> typing.Optional[
            Node]:
        pass

    def add_edge(self, edge: Edge, echo: bool = True) -> typing.Optional[Edge]:
        self.nquads += """
        _:{src} <e> _:{tgt} (type="{type}", value={value}) .
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
        self.mutate(self.nquads)
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
