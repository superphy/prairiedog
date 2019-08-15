import subprocess
import tempfile
import shutil
import logging
import time
import pathlib

import pydgraph

from prairiedog import debug_and_not_ci
from prairiedog.node import DEFAULT_NODE_TYPE
from prairiedog.dgraph import Dgraph, port

log = logging.getLogger('prairiedog')

offset = 0

with open("dgraph/kmers.schema") as f:
    KMERS_SCHEMA = ''.join(line for line in f)


def proc_error(p, msg: str):
    """Error handling for Dgraph processes"""
    log.fatal(msg)
    out, err = p.communicate()
    log.fatal("stdout:\n{}".format(out))
    log.fatal("stderr:\n{}".format(err))
    raise Exception(msg)


class DgraphBundled(Dgraph):
    """
    Helper to setup and tear-down dgraph
    """

    SCHEMA = """
    {}: string @index(exact) @upsert .
    """.format(DEFAULT_NODE_TYPE)

    def init_dgraph(self):
        if debug_and_not_ci():
            # Will display subprocess outputs
            pipes = {}
        else:
            # Will not display subprocess outputs
            pipes = {'stdout': subprocess.DEVNULL,
                     'stderr': subprocess.DEVNULL}

        log.info("Using global offset {}".format(offset))

        self._p_zero = subprocess.Popen(
            ['dgraph', 'zero', '-o', str(offset), '--wal', str(self.wal_dir)],
            cwd=str(self.out_dir),
            **pipes
        )
        time.sleep(2)
        # Should return None if still running
        if self._p_zero.poll() is not None:
            proc_error(self._p_zero, "Dgraph Zero failed to initialize")

        self._p_alpha = subprocess.Popen(
            ['dgraph', 'alpha', '--lru_mb', '2048', '--zero',
             'localhost:{}'.format(port("ZERO", offset)),
             '-o', str(offset), '--wal', str(self.wal_dir_alpha), '--postings',
             str(self.postings_dir)],
            cwd=str(self.out_dir),
            **pipes
        )
        time.sleep(4)
        if self._p_alpha.poll() is not None:
            proc_error(self._p_alpha, "Dgraph Alpha failed to initialize")

        if self.ratel:
            self._p_ratel = subprocess.Popen(
                ['dgraph-ratel', '-addr', 'localhost:{}'.format(
                    port("ZERO", offset))]
            )
            time.sleep(1)
            if self._p_ratel.poll() is not None:
                proc_error(self._p_ratel, "Dgraph Ratel failed to initialize")

    def set_schema(self):
        self.client.alter(pydgraph.Operation(schema=DgraphBundled.SCHEMA))
        self.client.alter(pydgraph.Operation(schema=KMERS_SCHEMA))

    def shutdown_dgraph(self):
        self._p_alpha.terminate()
        time.sleep(2)
        self._p_zero.terminate()
        time.sleep(2)
        if self._p_ratel is not None:
            self._p_ratel.terminate()

    def __init__(self, delete: bool = True, output_folder: str = None,
                 ratel: bool = False):
        # Ratel is the UI
        self.ratel = ratel
        self.delete = delete
        if output_folder is None:
            self.out_dir = tempfile.mkdtemp()
        else:
            self.out_dir = pathlib.Path(output_folder)
            self.out_dir.mkdir(parents=True, exist_ok=True)
        log.info("Will setup Dgraph from folder {}".format(self.out_dir))
        # Postings is only used by alpha
        self.postings_dir = pathlib.Path(self.out_dir, 'p')
        self.postings_dir.mkdir(parents=True, exist_ok=True)
        # This is the wal dir for zero
        self.wal_dir = pathlib.Path(self.out_dir, 'w')
        self.wal_dir.mkdir(parents=True, exist_ok=True)
        # Need separate wal for alpha
        self.wal_dir_alpha = pathlib.Path(self.out_dir, 'alpha', 'w')
        self.wal_dir_alpha.mkdir(parents=True, exist_ok=True)
        # Processes
        self._p_zero = None
        self._p_alpha = None
        self._p_ratel = None
        self.init_dgraph()
        global offset
        super().__init__(offset)
        offset += 1
        log.info("Set global offset to {}".format(offset))
        self.set_schema()

    def __del__(self):
        if self.delete:
            self.clear()
        time.sleep(2)
        self.shutdown_dgraph()
        if self.delete:
            log.warning("Wiping {} ...".format(self.out_dir))
            shutil.rmtree(self.out_dir)
        super().__del__()
