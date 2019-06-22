# Mostly from lemongraph/bench.py
import logging
from random import randint
from time import time
from concurrent.futures import ThreadPoolExecutor

from prairiedog.lemon_graph import LGGraph


log = logging.getLogger("prairiedog")


def munge(t, n):
    return t + str(n % 5)


mil = tuple(range(0, 1000000))
pairs = set()
while len(pairs) < 1000000:
    pairs.add((randint(0, 999999), randint(0, 999999)))
pairs = sorted(pairs)


def _do_graphing(txn):
    """
    :txn:
    :return:
    """
    for run in (1, 2, 3):
        times = [time()]

        start = times[-1]
        nodes = [txn.node(type=munge('node', x), value=x) for x in mil]
        log.info("+1m nodes")
        if run == 1:
            elapsed = times[-1] - start
            print("total node insert time: %.3lf" % elapsed)
            print("total node insert rate: %.3lf" % (1000000 / elapsed))
            txn.commit()

        start = times[-1]
        for x, n in enumerate(nodes):
            n[munge('prop', x)] = munge('value', x)
        log.info("+1m props")
        if run == 2:
            elapsed = times[-1] - start
            print("total prop insert time: %.3lf" % elapsed)
            print("total prop insert rate: %.3lf" % (1000000 / elapsed))
            txn.commit()

        start = times[-1]
        for i, x_y in enumerate(pairs):
            x, y = x_y
            e = txn.edge(type=munge('edge', x + y), value=i, src=nodes[x],
                         tgt=nodes[y])
        log.info("+1m edges")
        elapsed = times[-1] - start
        log.info("total edge insert time: %.3lf" % elapsed)
        log.info("total edge insert rate: %.3lf" % (1000000 / elapsed))

    assert True


def test_no_concurrency_lemongraph_task(lgr: LGGraph):
    """
    Check that the basic task works.
    :return:
    """
    g = lgr.g
    with g.transaction(write=True) as txn:
        _do_graphing(txn)


def _spawn_txn(g):
    ctx = g.transaction(write=True)
    txn = ctx.__enter__()
    yield txn
    ctx.__exit__(None, None, None)


def test_concurrency_lemongraph_txn_threads(lgr: LGGraph):
    g = lgr.g
    pass
