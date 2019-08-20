import logging

from prairiedog.graph import Graph
from prairiedog.dgraph_bundled import DgraphBundled, proc_error

log = logging.getLogger("prairiedog")


class Error(Exception):
    """
    Base class for custom exceptions
    """


class GraphException(Error):
    """
    Generic error for some problem with LemonGraph
    """
    def __init__(self, g: Graph):
        super(GraphException, self).__init__(
            "Problem with Graph with nodes {} and edges {}".format(
                g.nodes, g.edges))
        log.error("Dumping nodes:")
        for node in g.nodes:
            log.error(node)
        log.error("Dumping edges:")
        for edge in g.edges:
            log.error(edge)


class DgraphBundledException(GraphException):
    """
    For handling our subprocess exceptions.
    """
    def __init__(self, g: DgraphBundled):
        if g._p_zero is not None:
            proc_error(g._p_zero, "Dgraph Zero error")
        if g._p_alpha is not None:
            proc_error(g._p_alpha, "Dgraph Alpha error")
        if g._p_ratel is not None:
            proc_error(g._p_ratel, "Dgraph Ratel error")
        super(DgraphBundledException, self).__init__(g)
