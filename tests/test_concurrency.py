# Mostly from lemongraph/bench.py
import logging
import os
from random import randint
from time import time
from concurrent.futures import Executor, ThreadPoolExecutor, as_completed

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
            print("total node insert rate: %.3lf" % (1000000.0 / elapsed))
            txn.commit()

        start = times[-1]
        for x, n in enumerate(nodes):
            n[munge('prop', x)] = munge('value', x)
        log.info("+1m props")
        if run == 2:
            elapsed = times[-1] - start
            print("total prop insert time: %.3lf" % elapsed)
            print("total prop insert rate: %.3lf" % (1000000.0 / elapsed))
            txn.commit()

        start = times[-1]
        for i, x_y in enumerate(pairs):
            x, y = x_y
            e = txn.edge(type=munge('edge', x + y), value=i, src=nodes[x],
                         tgt=nodes[y])
        log.info("+1m edges")
        elapsed = times[-1] - start
        log.info("total edge insert time: %.3lf" % elapsed)
        log.info("total edge insert rate: %.3lf" % (1000000.0 / elapsed))

    assert True
    return True


def _do_basic_graphing(txn):
    log.info("Doing graphing in pid {}".format(os.getpid()))
    a = txn.node(type='n', value='1')
    b = txn.node(type='n', value='2')
    c = txn.node(type='n', value='3')

    e1 = txn.edge(src=a, tgt=b, type='x', value='y')
    e1['incr'] = 0

    e2 = txn.edge(src=b, tgt=c, type='x', value='y')
    e2['incr'] = 1

    assert True
    return True


def test_no_concurrency_lemongraph_task(lgr: LGGraph):
    """
    Check that the basic task works.
    :return:
    """
    g = lgr.g
    with g.transaction(write=True) as txn:
        _do_basic_graphing(txn)


def _do_concurrency(lgr: LGGraph, workers: int, executor: Executor,
                    timeout: int = 10):
    log.info("Spinning up concurrency tasks in pid {}".format(os.getpid()))
    g = lgr.g

    ctxs = [g.transaction(write=True) for _ in range(workers)]
    txns = [ctxs[i].__enter__() for i in range(workers)]

    futures = {
        executor.submit(_do_basic_graphing, txns[i]): ctxs[i]
        for i in range(len(txns))
    }
    for future in as_completed(futures, timeout=timeout):
        try:
            data = future.result()
            log.info(('Future result {}'.format(data)))
        except Exception as exc:
            log.fatal('Future generated an exception: %s' % exc)
            raise exc
        else:
            ctx = futures[future]
            ctx.__exit__(None, None, None)


def test_concurrency_lemongraph_txn_threads(lgr: LGGraph, n_workers: int):
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        _do_concurrency(lgr, n_workers, executor)

