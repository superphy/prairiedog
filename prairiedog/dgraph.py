import logging
import typing
import time
import pathlib
import json
import sys
import hashlib

import pydgraph
import grpc


from prairiedog.edge import Edge
from prairiedog.graph import Graph
from prairiedog.node import Node, DEFAULT_NODE_TYPE
from prairiedog.kmers import possible_kmers

log = logging.getLogger("prairiedog")

DGRAPH_HOST = 'localhost'

# This is specific to Dgraph
DEFAULT_EDGE_PREDICATE = 'fd'


def port(component: str, offset: int = 0) -> int:
    if component == "ZERO":
        # gRPC port
        return 5080 + offset
    elif component == "ZERO_HTTP":
        return 6080 + offset
    elif component == "ALPHA":
        # gRPC port
        return 9080 + offset
    elif component == "ALPHA_HTTP":
        return 8080 + offset
    elif component == "RATEL":
        # HTTP port
        log.debug("Ratel port isn't controlled with an offset")
        return 8000


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
            log.debug("Node not found, adding nquads \n{}".format(nquads))
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
            q(func: eq({node_type}, "{src}")) @cascade {{
                {edge_predicate} @filter(
                        eq(type, "{facet_type}") AND eq(value, {facet_value})
                    ) {{
                    uid
                    {edge_predicate} @filter(eq({node_type}, "{tgt}"))
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
            log.warning("upsert_edge() was called with existing edge:")
            log.warning(str(edge))
            return
        else:
            uid_a = self.upsert_node(
                Node(node_type=node_type, value=edge.src), echo=True).db_id
            uid_b = self.upsert_node(
                Node(node_type=node_type, value=edge.tgt), echo=True).db_id
            nquads = """
            <{a}> <{ep}> _:e .
            _:e <{ep}> <{b}> .
            _:e <type> "{edge_type}" .
            _:e <value> "{edge_value}" .
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
    def _parse_edge(src, d, node_type: str = DEFAULT_NODE_TYPE,
                    edge_predicate: str = DEFAULT_EDGE_PREDICATE) -> Edge:
        e = Edge(src=src, tgt="")
        for k, v in d.items():
            if k == edge_predicate:
                e.tgt = v[0][node_type]
            elif "type" in k:
                e.edge_type = v
            elif "value" in k:
                e.edge_value = v
            elif k == "uid":
                e.db_id = v
        return e

    @staticmethod
    def _parse_edges(list_edges: list, node_type: str,
                     edge_predicate: str, src: str = None) -> tuple:
        st = set()
        src_arg = src
        for d in list_edges:
            if src_arg is None:
                src = d[node_type]
            else:
                src = src_arg
            edges_list = d[edge_predicate]
            for edge_dict in edges_list:
                e = Dgraph._parse_edge(src, edge_dict, node_type,
                                       edge_predicate)
                st.add(e)

        return tuple(st)

    @staticmethod
    def _parse_edges_reverse(list_edges: list, node_type: str,
                             edge_predicate: str, src: str = None) -> tuple:
        st = set()
        for d in list_edges:
            if src is None:
                src = d[node_type]
            e = Dgraph._parse_edge(src, d, node_type,
                                   edge_predicate)
            st.add(e)

        return tuple(st)

    @property
    def edges(self) -> typing.Set[Edge]:
        query = """
        {{
            q(func: has({nt})) @filter(has({et})) {{
                expand(_all_) {{
                    uid
                    expand(_all_) {{
                        {nt}
                    }}
                }}
            }}
        }}
        """.format(nt=DEFAULT_NODE_TYPE, et=DEFAULT_EDGE_PREDICATE)
        r = self.query(query)

        if len(r['q']) == 0:
            return set()

        return self._parse_edges(
            r['q'], DEFAULT_NODE_TYPE, DEFAULT_EDGE_PREDICATE)

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
                time.sleep(2 ** depth)
                self.mutate(nquads=nquads, depth=depth + 1)
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

    def find_edges(self, node_value: str) -> tuple:
        query = """
        {{
            q(func: eq({nt}, "{nv}")) @filter(has({et})) {{
                expand(_all_) {{
                    uid
                    expand(_all_) {{
                        {nt}
                    }}
                }}
            }}
        }}
        """.format(nv=node_value, nt=DEFAULT_NODE_TYPE,
                   et=DEFAULT_EDGE_PREDICATE)
        r = self.query(query)
        if len(r['q']) == 0:
            return tuple()
        return self._parse_edges(
            list_edges=r['q'], node_type=DEFAULT_NODE_TYPE,
            edge_predicate=DEFAULT_EDGE_PREDICATE)

    def find_edges_reverse(self, node_value: str) -> tuple:
        exists, uid = self.exists_node(Node(value=node_value))
        if not exists:
            log.warning("node_value {} doesn't exist".format(node_value))
            return tuple()
        # Find the uid of the edge
        query = """
        {{
            q(func: has({et})) @filter(uid_in({et}, {tgt})) {{
                uid
            }}
        }}
        """.format(et=DEFAULT_EDGE_PREDICATE, tgt=uid)
        r = self.query(query)
        if len(r['q']) == 0:
            return tuple()
        edge_uid = r['q'][0]['uid']

        # Find the source edge
        query_2 = """
        {{
            q(func: has({nt})) @filter(uid_in({et}, {uid})) {{
                {nt}
            }}
        }}
        """.format(nt=DEFAULT_NODE_TYPE, et=DEFAULT_EDGE_PREDICATE,
                   uid=edge_uid)
        r_2 = self.query(query_2)
        src = r_2['q'][0][DEFAULT_NODE_TYPE]

        # Finally, query the edge
        query_3 = """
        {{
            q(func: has({et})) @filter(uid_in({et}, {tgt})) {{
                expand(_all_) {{
                    uid
                    expand(_all_) {{
                        {nt}
                    }}
                }}
            }}
        }}
        """.format(nv=node_value, nt=DEFAULT_NODE_TYPE,
                   et=DEFAULT_EDGE_PREDICATE, tgt=uid)
        r_3 = self.query(query_3)
        if len(r_3['q']) == 0:
            return tuple()

        return self._parse_edges_reverse(
            list_edges=r_3['q'], node_type=DEFAULT_NODE_TYPE,
            edge_predicate=DEFAULT_EDGE_PREDICATE, src=src)

    def connected(self, node_a: str, node_b: str) -> typing.Tuple[
            bool, typing.Tuple]:
        edges_a = self.find_edges(node_a)
        edges_b = self.find_edges_reverse(node_b)
        matched, src_edges = Dgraph.matching_edges(edges_a, edges_b)
        if matched is not True:
            return False, ()
        else:
            log.info("Found {} connections between {} and {}".format(
                len(src_edges), node_a, node_b
            ))
            log.debug("src_edges are {}".format(src_edges))
            return True, src_edges

    @staticmethod
    def _parse_types(l: list, ep: str) -> typing.Set[str]:
        st = set()
        for d in l:
            t = d[ep][0]["type"]
            st.add(t)
        return st

    def find_value(self, uid: str, t: str) -> int:
        query = """
        {{
            q(func: uid({uid})) {{
                {ep} {{
                    value
                    @filter(eq(type, "{et}"))
                }}
            }}
        }}
        """.format(uid=uid, ep=DEFAULT_EDGE_PREDICATE, et=t)
        r = self.query(query)
        if len(r["q"]) == 0:
            return -1
        return r["q"][0][DEFAULT_EDGE_PREDICATE][0]["value"]

    def find_value_reverse(self, uid: str, t: str) -> int:
        query = """
        {{
            q(func: has({ep})) @filter(uid_in({ep}, {uid})) {{
                value
                @filter(eq(type, "{et}"))
            }}
        }}
        """.format(nt=DEFAULT_NODE_TYPE, ep=DEFAULT_EDGE_PREDICATE, uid=uid,
                   et=t)
        r = self.query(query)
        if len(r["q"]) == 0:
            return -1
        return r["q"][0]["value"]

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

    @staticmethod
    def _path_query(node_type: str, node_value: str, edge_predicate: str,
                    edge_type: str, start_int: int, end_int: int) -> str:
        s = '{{q(func: eq({nt}, "{nv}")) {{ {nt} '.format(
            nt=node_type, nv=node_value)
        for ix in range(start_int, end_int + 1):
            # The spaces in this query are very important, so ignore line len
            s += '{ep} @filter(eq(type, "{et}") AND eq(value, {v})) {{ {ep} {{ {nt} '.format(  # noqa: E501
                ep=edge_predicate, et=edge_type, v=ix, nt=node_type)
        for ix in range(start_int, end_int + 1):
            s += '}}'
        s += '}}'
        return s

    @staticmethod
    def _parse_path(d: dict, node_type: str, edge_predicate: str) -> tuple:
        lt = [Node(value=d[node_type])]
        while True:
            if edge_predicate not in d:
                break
            d = d[edge_predicate][0][edge_predicate][0]
            v = d[node_type]
            lt.append(Node(value=v))
        return tuple(lt)

    def path(self, node_a: str, node_b: str) -> typing.Tuple[tuple, tuple]:
        log.info("Checking all paths between {} and {}".format(node_a, node_b))
        exists, uid_a = self.exists_node(Node(value=node_a))
        if not exists:
            return tuple(), tuple()
        exists, uid_b = self.exists_node(Node(value=node_b))
        if not exists:
            return tuple(), tuple()
        log.debug("Both nodes exist, checking connectivity...")

        connected, src_edges = self.connected(node_a, node_b)
        if not connected:
            return tuple(), tuple()

        log.debug("Nodes are connected")
        paths = []
        paths_meta = []

        for src_edge in src_edges:
            log.info("Finding path between {} and {} with source edge {}"
                     "".format(node_a, node_b, src_edge))
            tgt_edges = self.find_edges_reverse(node_b)
            for tgt_edge in tgt_edges:
                if tgt_edge.edge_type != src_edge.edge_type or \
                        tgt_edge.edge_value < src_edge.edge_value:
                    log.info("Skipping tgt edge {} for src edge {}".format(
                        tgt_edge, src_edge
                    ))
                    continue
                log.debug("Checking path for type: {}".format(
                    tgt_edge.edge_type))
                log.debug("Found start value of {} and end value of {}".format(
                    src_edge.edge_value, tgt_edge.edge_value))
                query = self._path_query(
                    node_type=DEFAULT_NODE_TYPE, node_value=node_a,
                    edge_predicate=DEFAULT_EDGE_PREDICATE,
                    edge_type=src_edge.edge_type,
                    start_int=src_edge.edge_value, end_int=tgt_edge.edge_value)
                r = self.query(query)
                log.info(r)
                if len(r['q']) != 1:
                    log.warning("Path not found for type: {}".format(
                        src_edge.edge_type
                    ))
                    continue
                p = self._parse_path(
                    r['q'][0], DEFAULT_NODE_TYPE, DEFAULT_EDGE_PREDICATE)
                paths.append(p)
                paths_meta.append({'edge_type': tgt_edge.edge_type})
        return tuple(paths), tuple(paths_meta)


class DgraphBulk(Graph):
    def __init__(self):
        self.nquads = ""

    def upsert_node(self, node: Node, echo: bool = True) -> typing.Optional[
            Node]:
        pass

    def preload(self, k: int = 11):
        self.nquads = '\n'.join(
            '_:{kmer} <{nt}> "{kmer}" .'.format(
                kmer=kmer, nt=DEFAULT_NODE_TYPE
            )
            for kmer in possible_kmers(k)
        )

    def add_edge(self, edge: Edge, echo: bool = True) -> typing.Optional[Edge]:
        # The hash is used to ensure blank nodes are unique before assignment
        e = hashlib.sha1("{}{}{}{}".format(
            edge.src, edge.tgt, edge.edge_type, edge.edge_value).encode(
            "utf-8")).hexdigest()
        self.nquads += """
        _:{src} <{et}> _:{e} .
        _:{e} <{et}> _:{tgt} .
        _:{e} <type> "{fl}" .
        _:{e} <value> "{fv}" .
        """.format(src=edge.src, tgt=edge.tgt, fl=edge.edge_type,
                   fv=edge.edge_value, et=DEFAULT_EDGE_PREDICATE, e=e)

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
