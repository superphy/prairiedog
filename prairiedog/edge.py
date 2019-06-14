class Edge:
    """
    Defines a set structure for creating edges
    """
    def __init__(self, src: str, tgt: str, edge_type: str = 'e',
                 edge_value: str = 'und', incr: int = None,
                 labels: dict = None):
        self.src = src
        self.tgt = tgt
        self.edge_type = edge_type
        self.edge_value = edge_value
        self.incr = incr
        self.labels = labels

    @property
    def origin(self):
        return "{}_{}".format(self.edge_type, self.edge_value)
