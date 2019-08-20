import logging
import tempfile
import pathlib

from prairiedog.dgraph_bundled import DgraphBundled
from dgraph.bulk import run_dgraph_bulk


log = logging.getLogger('prairiedog')


class DgraphBundledHelper:
    """For loading arbitrary rdf and testing"""
    def __init__(self, out_dir: str = None):
        # We have to create the first DgraphBundled instance in a temporary
        # directory and copy the postings from Dgraph Bulk over. Otherwise,
        # the Dgraph subprocesses will fail to start if initialized from the
        # same directory.
        self.tmp_output = tempfile.mkdtemp()
        if out_dir is not None:
            self.final_output = pathlib.Path(out_dir).resolve()
        else:
            self.final_output = pathlib.Path(tempfile.mkdtemp()).resolve()
        self._g = None

    def load(self, rdf_dir: str, delete_after: bool = True) -> pathlib.Path:
        log.info("Loading rdf from {} ...".format(rdf_dir))
        p = pathlib.Path(self.tmp_output, 'dgraph')
        p_final = pathlib.Path(self.final_output, 'dgraph')
        p_final_postings = pathlib.Path(p_final, 'p')
        self._g = DgraphBundled(delete=False, output_folder=p)
        run_dgraph_bulk(cwd=p, move_to=p_final_postings,
                        rdfs=rdf_dir, zero_port=self.g.zero_port)
        # Reinitialize DgraphBundled after moving postings files
        self._g.shutdown_dgraph()
        del self._g
        self._g = DgraphBundled(delete=delete_after,
                                output_folder=p_final_postings)
        return p

    @property
    def g(self) -> DgraphBundled:
        if self._g is not None:
            return self._g
        else:
            msg = "load() must be called before accessing g"
            log.critical(msg)
            raise AttributeError(msg)
