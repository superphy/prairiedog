import subprocess
import tempfile
import shutil
import logging
import time
import pathlib

import pydgraph
import grpc
import psutil

from prairiedog import debug_and_not_ci
from prairiedog.node import DEFAULT_NODE_TYPE
from prairiedog.dgraph import Dgraph, port
from prairiedog.errors import GraphException, SubprocessException

log = logging.getLogger('prairiedog')

offset = 0

with open("dgraph/kmers.schema") as f:
    KMERS_SCHEMA = ''.join(line for line in f)


def recommended_lru() -> int:
    vm_bytes = psutil.virtual_memory().total
    vm_mb = vm_bytes / (1 << 20)
    lru_mb = int(vm_mb / 3)
    return lru_mb


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
            log.info("Debug mode is set - will directly output Dgraph logs")
            pipes_zero = {}
            pipes_alpha = {}
        else:
            # Will not display subprocess outputs
            self.subprocess_log_file_zero = pathlib.Path(
                self.out_dir, 'dgraph_zero.log')
            self.subprocess_log_file_alpha = pathlib.Path(
                self.out_dir, 'dgraph_alpha.log')
            log.info(
                "Debug mode not set:\nAlpha logs: {}\nZero logs: {}".format(
                    self.subprocess_log_file_zero,
                    self.subprocess_log_file_alpha
                ))
            self.subprocess_log_zero = open(
                self.subprocess_log_file_zero, 'a')
            self.subprocess_log_alpha = open(
                self.subprocess_log_file_alpha, 'a')
            pipes_zero = {
                'stdout': self.subprocess_log_zero,
                'stderr': self.subprocess_log_zero}
            pipes_alpha = {
                'stdout': self.subprocess_log_alpha,
                'stderr': self.subprocess_log_alpha}

        log.info("Using local offset {}".format(self.offset))

        # Calculate recommended size of LRU cache
        if self.deploy:
            lru_mb = recommended_lru()
        else:
            lru_mb = 2048

        self._p_zero = subprocess.Popen(
            ['dgraph', 'zero', '-o', str(self.offset), '--wal',
             str(self.wal_dir)],
            cwd=str(self.out_dir),
            **pipes_zero
        )
        time.sleep(2)
        # Should return None if still running
        if self._p_zero.poll() is not None:
            raise SubprocessException(
                self._p_zero, "Dgraph Zero failed to initialize")
        else:
            self.zero_port = port("ZERO", self.offset)

        self._p_alpha = subprocess.Popen(
            ['dgraph', 'alpha', '--lru_mb', str(lru_mb), '--zero',
             'localhost:{}'.format(self.zero_port),
             '-o', str(self.offset), '--wal', str(self.wal_dir_alpha),
             '--postings', str(self.postings_dir)],
            cwd=str(self.out_dir),
            **pipes_alpha
        )
        time.sleep(4)
        if self._p_alpha.poll() is not None:
            raise SubprocessException(
                self._p_alpha, "Dgraph Alpha failed to initialize")
        else:
            self.alpha_port = port("ALPHA", self.offset)

        if self.ratel:
            self._p_ratel = subprocess.Popen(
                ['dgraph-ratel', '-addr', 'localhost:{}'.format(
                    self.zero_port)]
            )
            time.sleep(1)
            if self._p_ratel.poll() is not None:
                raise SubprocessException(
                    self._p_ratel, "Dgraph Ratel failed to initialize")
            self.ratel_port = port("RATEL")  # This is not via offset

    def log_ports(self):
        # Log ports
        log.info("Initialized Dgraph instance:")
        if self.zero_port:
            log.info("Dgraph Zero gRPC port     : {}".format(self.zero_port))
            log.info("Dgraph Zero HTTP port     : {}".format(
                port("ZERO_HTTP", self.offset)))
        if self.alpha_port:
            log.info("Dgraph Alpha gRPC port    : {}".format(self.alpha_port))
            log.info("Dgraph Alpha HTTP port    : {}".format(
                port("ALPHA_HTTP", self.offset)))
        if self.ratel_port:
            log.info("Dgraph Ratel HTTP port    : {}".format(self.ratel_port))
            log.info("Note: Ratel should connect to port {}".format(
                port("ALPHA_HTTP", self.offset)))

    def set_schema(self):
        log.info("Setting dgraph schema...")
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
                 ratel: bool = False, deploy: bool = False, delay: int = 10):
        # Ratel is the UI
        self.ratel = ratel
        self.delete = delete
        self.deploy = deploy
        if output_folder is None:
            self.out_dir = tempfile.mkdtemp()
        else:
            self.out_dir = pathlib.Path(output_folder).resolve()
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
        # Optional logs
        self.subprocess_log_file_zero = None
        self.subprocess_log_file_alpha = None
        self.subprocess_log_zero = None
        self.subprocess_log_alpha = None
        # Ports
        self.zero_port = None
        self.alpha_port = None
        self.ratel_port = None
        global offset
        self.offset = offset
        log.info("Claiming offset {} for local offset".format(offset))
        offset += 1
        log.info("Set global offset to {}".format(offset))
        # Init dgraph
        self.init_dgraph()
        super().__init__(self.offset)
        self.log_ports()
        try:
            self.set_schema()
        except grpc.RpcError as rpc_error_call:
            log.warning("Ran into exception {}, will retry...".format(
                rpc_error_call
            ))
            # In the case that Dgraph hasn't initialized yet
            time.sleep(delay)
            log.warning("Retying to set schema...")
            try:
                self.set_schema()
            except grpc.RpcError as rpc_error_call:
                log.critical("Ran into the a RpcError again: {}".format(
                    rpc_error_call
                ))
                raise DgraphBundledException(self)

    def __del__(self):
        if self.delete:
            self.clear()
        time.sleep(2)
        self.shutdown_dgraph()
        if self.delete:
            log.warning("Wiping {} ...".format(self.out_dir))
            shutil.rmtree(str(self.out_dir))
        if self.subprocess_log_zero is not None:
            self.subprocess_log_zero.close()
        if self.subprocess_log_alpha is not None:
            self.subprocess_log_alpha.close()
        super().__del__()


class DgraphBundledException(GraphException):
    """
    For handling our subprocess exceptions.
    """
    def __init__(self, g: DgraphBundled):
        log.critical("DgraphBundled encountered an exception")
        if g.subprocess_log_file_zero is not None:
            with open(g.subprocess_log_file_zero) as fz:
                log.critical("Dgraph Zero logs:\n{}".format(fz.read()))
        if g.subprocess_log_file_alpha is not None:
            with open(g.subprocess_log_file_alpha) as fa:
                log.critical("Dgraph Alpha logs:\n{}".format(fa.read()))
        super(DgraphBundledException, self).__init__(g)
