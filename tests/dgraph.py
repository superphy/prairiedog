import subprocess
import tempfile
import shutil
import logging
import time

import pydgraph

from prairiedog.node import DEFAULT_TYPE
from prairiedog.dgraph import Dgraph

log = logging.getLogger('prairiedog')


class DG(Dgraph):
    """
    Helper to setup and tear-down dgraph
    """

    SCHEMA = """
    {}: string @index(exact) @upsert .
    """.format(DEFAULT_TYPE)

    def init_dgraph(self):
        self._p_zero = subprocess.Popen(['dgraph', 'zero'], cwd=self.tmp_dir)
        time.sleep(2)
        self._p_alpha = subprocess.Popen(
            ['dgraph', 'alpha', '--lru_mb', '2048', '--zero',
             'localhost:5080'], cwd=self.tmp_dir)
        time.sleep(4)

    def set_schema(self):
        self.client.alter(pydgraph.Operation(schema=DG.SCHEMA))

    def shutdown_dgraph(self):
        self._p_zero.terminate()
        self._p_alpha.terminate()
        time.sleep(2)

    def __init__(self):
        self.tmp_dir = tempfile.mkdtemp()
        log.info("Will setup Dgraph from folder {}".format(self.tmp_dir))
        self._p_zero = None
        self._p_alpha = None
        self.init_dgraph()
        super().__init__()
        self.set_schema()

    def __del__(self):
        self.clear()
        time.sleep(2)
        self.shutdown_dgraph()
        shutil.rmtree(self.tmp_dir)
        super().__del__()
