import logging

from prairiedog.graph import Graph

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
        super(GraphException, self).__init__("Problem with Graph with nodes {}\
        and edges {}".format(g.nodes, g.edges))
        log.error("Dumping nodes:")
        for node in g.nodes:
            log.error(node)
        log.error("Dumping edges:")
        for edge in g.edges:
            log.error(edge)