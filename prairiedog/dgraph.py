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

DGRAPH_HOST = 'localhost'
SCHEME = ''

# This is specific to Dgraph
DEFAULT_EDGE_PREDICATE = 'fd'


def port(component: str, offset: int = 0) -> int:
    if component == "ZERO":
        return 5080 + offset
    elif component == "ALPHA":
        return 9080 + offset


def decode(b: bytes):
    if sys.version_info < (3, 6):
        return json.loads(b.decode('utf-8'))
    else:
        return json.loads(b)


class Dgraph(Graph):

    def __init__(self, port_offset: int = 0):
        self.dgraph_url = "{}:{}".format(
            DGRAPH_HOST, port("ALPHA", port_offset))
        log.info("Will connect to Dgraph instance at {}".format(
            self.dgraph_url))
        self._client_stub = None
        self._client = None
        self.nquads = ""
        log.debug("Done initializing Dgraph client")

    @property
    def client_stub(self):
        if self._client_stub is None:
            self._client_stub = pydgraph.DgraphClientStub(self.dgraph_url)
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
    def _exists_node(r) -> typing.Tuple[bool, str]:
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
        return Dgraph._exists_node(r)

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

    @staticmethod
    def _exists_edge(r) -> typing.Tuple[bool, str]:
        if len(r['q']) != 0:
            assert len(r['q'][0]['fd']) == 1
            return True, r['q'][0]['fd'][0]['uid']
        else:
            return False, ""

    def exists_edge(self, edge: Edge, node_type: str = None,
                    edge_predicate: str = None) -> typing.Tuple[bool, str]:
        log.debug("Checking if edge exists...")
        if node_type is None:
            node_type = DEFAULT_NODE_TYPE
        if edge_predicate is None:
            edge_predicate = DEFAULT_EDGE_PREDICATE
        query = """{{
            q(func: eq({node_type}, "{src}")) {{
                {edge_predicate} @facets(
                    eq(type, "{facet_type}") AND eq(value, {facet_value})
                ) @filter(eq({node_type}, "{tgt}")) {{
                    uid
                }}
            }}
        }}
        """.format(node_type=node_type, src=edge.src,
                   edge_predicate=edge_predicate, facet_type=edge.edge_type,
                   facet_value=edge.edge_value, tgt=edge.tgt)
        r = self.query(query)
        return Dgraph._exists_edge(r)

    def upsert_edge(self, edge: Edge, node_type: str = None,
                    edge_predicate: str = None):
        log.debug("Upserting edge...")
        if node_type is None:
            node_type = DEFAULT_NODE_TYPE
        if edge_predicate is None:
            edge_predicate = DEFAULT_EDGE_PREDICATE
        exists, uid = self.exists_edge(edge, node_type=node_type,
                                       edge_predicate=edge_predicate)
        if exists:
            return
        else:
            uid_a = self.upsert_node(
                Node(node_type=node_type, value=edge.src), echo=True).db_id
            uid_b = self.upsert_node(
                Node(node_type=node_type, value=edge.tgt), echo=True).db_id
            nquads = """
            <{a}> <{ep}> <{b}> (type="{edge_type}", value={edge_value}) .
            """.format(a=uid_a, b=uid_b,
                       ep=edge_predicate, edge_type=edge.edge_type,
                       edge_value=edge.edge_value)
            log.debug("Edge not found, adding nquad \n{}".format(nquads))
            self.mutate(nquads)

    def add_edge(self, edge: Edge, echo: bool = True) -> typing.Optional[Edge]:
        return self.upsert_edge(edge)

    def clear(self):
        op = pydgraph.Operation(drop_all=True)
        self.client.alter(op)

    @staticmethod
    def _parse_node(d: dict) -> Node:
        n = Node(value="")
        for k, v in d.items():
            if k == DEFAULT_NODE_TYPE:
                n.value = v
            elif k == "uid":
                n.db_id = v
        return n

    @property
    def nodes(self) -> typing.Set[Node]:
        query = """
        {{
          q(func: has({type})){{
                uid
                expand(_all_)
          }}
        }}
        """.format(type=DEFAULT_NODE_TYPE)
        r = self.query(query)

        if len(r['q']) == 0:
            return set()

        st = set()
        for d in r['q']:
            st.add(Dgraph._parse_node(d))
        return st

    @staticmethod
    def _parse_edge(src, d):
        e = Edge(src=src, tgt="")
        for k, v in d.items():
            if k == DEFAULT_NODE_TYPE:
                e.tgt = v
            elif "type" in k:
                e.edge_type = v
            elif "value" in k:
                e.edge_value = v
            elif k == "uid" :
                e.db_id = v
        return e

    @property
    def edges(self) -> typing.Set[Edge]:
        query = """
        {{
            q(func: has({nt})) @filter(has({et})) {{
                expand(_all_) {{
                    uid
                    expand(_all_)
                }}
            }}
        }}
        """.format(nt=DEFAULT_NODE_TYPE, et=DEFAULT_EDGE_PREDICATE)
        r = self.query(query)

        if len(r['q']) == 0:
            return set()

        st = set()
        for d in r['q']:
            src = d[DEFAULT_NODE_TYPE]
            edges = d[DEFAULT_EDGE_PREDICATE]
            for ed in edges:
                e = Dgraph._parse_edge(src, ed)
                st.add(e)
        return st

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
            if depth < max_depth:
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
        pass

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

    @staticmethod
    def _parse_types(l: list, ep: str) -> typing.Set[str]:
        st = set()
        for d in l:
            t = d[ep][0]["{}|type".format(ep)]
            st.add(t)
        return st

    def find_value(self, uid: str, t: str) -> int:
        query = """
        {{
            q(func: uid({uid})) {{
                {ep} @facets(eq(type, "{et}")) @facets(value)
            }}
        }}
        """.format(uid=uid, ep=DEFAULT_EDGE_PREDICATE, et=t)
        r = self.query(query)
        if len(r["q"]) == 0:
            return -1
        return r["q"][0][DEFAULT_EDGE_PREDICATE][0][
            "{}|value".format(DEFAULT_EDGE_PREDICATE)]

    def find_value_reverse(self, uid: str, t: str) -> int:
        query = """
        {{
            q(func: has({nt})) @filter(uid_in({ep}, {uid})) {{
                {ep} @facets(eq(type, "{t}")) @facets(value)
            }}  
        }}
        """.format(nt=DEFAULT_NODE_TYPE, ep=DEFAULT_EDGE_PREDICATE, uid=uid,
                   t=t)
        r = self.query(query)
        if len(r["q"]) == 0:
            return -1
        return r["q"][0][DEFAULT_EDGE_PREDICATE][0][
            "{}|value".format(DEFAULT_EDGE_PREDICATE)]

    def find_depth(self, uid_a: str, uid_b: str, t: str) -> int:
        """
        Depth is used for prevent queries from getting stuck in a cycle.
        :param uid_a:
        :param uid_b:
        :param t:
        :return:
        """
        va = self.find_value(uid_a, t)
        if va == -1:
            return -1
        vb = self.find_value_reverse(uid_b, t)
        if vb == -1:
            return -1
        depth = vb - va
        # Depth is in number of hops, so +1
        return depth+1

    def query_paths(self, uid_a: str, uid_b: str, t: str,
                    n: int = 100) -> tuple:
        log.debug("Querying path between {} and {}".format(uid_a, uid_b))
        depth = self.find_depth(uid_a, uid_b, t)
        query = """
        {{
            path as shortest(
                from:{uid_a}, to:{uid_b}, numpaths:{n} depth:{d}) {{
                    {ep} @facets(eq(type, "{t}"))
            }}
            path(func: uid(path)) {{
                {nt}
            }}
        }}
        """.format(nt=DEFAULT_NODE_TYPE, ep=DEFAULT_EDGE_PREDICATE,
                   uid_a=uid_a, uid_b=uid_b, t=t, n=n, d=depth)
        r = self.query(query)
        log.info("query_paths() got {}".format(r))

    def path(self, node_a: str, node_b: str) -> tuple:
        log.info("Checking all paths between {} and {}".format(node_a, node_b))
        exists, uid_a = self.exists_node(Node(value=node_a))
        if not exists:
            return tuple()
        exists, uid_b = self.exists_node(Node(value=node_b))
        if not exists:
            return tuple()
        log.info("Both nodes exist, checking types...")
        # We first have to find all "type"(s) of edges there are; these are the
        # source genomes
        query_et = """
        {{
            q(func: eq({nt}, "{src}")){{
                {ep} @facets(type)
            }}
        }}
        """.format(src=node_a, nt=DEFAULT_NODE_TYPE, ep=DEFAULT_EDGE_PREDICATE)
        r = self.query(query_et)
        if len(r["q"]) == 0:
            log.info("No types found, returning...")
            return tuple()
        types = self._parse_types(r["q"], DEFAULT_EDGE_PREDICATE)
        log.info("Found types as: {}".format(types))
        paths = tuple()
        for t in types:
            log.info("Checking path for type: {}".format(t))
            p = self.query_paths(uid_a, uid_b, t)
            paths += p


class DgraphBulk(Graph):
    def upsert_node(self, node: Node, echo: bool = True) -> typing.Optional[
            Node]:
        pass

    def add_edge(self, edge: Edge, echo: bool = True) -> typing.Optional[Edge]:
        self.nquads += """
        _:{src} <{et}> _:{tgt} (type="{fl}", value={fv}) .
        """.format(src=edge.src, tgt=edge.tgt, fl=edge.edge_type,
                   fv=edge.edge_value, et=DEFAULT_EDGE_PREDICATE)

    def clear(self):
        pass

    @property
    def nodes(self) -> typing.Set[Node]:
        pass

    @property
    def edges(self) -> typing.Set[Edge]:
        pass

    def get_labels(self, node: str) -> dict:
        pass

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
