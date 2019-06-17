class Edge:
    """
    Defines a set structure for creating edges
    """
    def __init__(self, src: str, tgt: str, edge_type: str = 'e',
                 edge_value: str = 'und', incr: int = None,
                 labels: dict = None, db_id=None):
        self.src = src
        self.tgt = tgt
        # Note: LemonGraph uses the type + value as str when adding, but
        # returns ordinal IDs when called
        self.edge_type = edge_type
        self.edge_value = edge_value
        if incr is None and labels is not None and 'incr' in labels:
            self.incr = labels['incr']
            labels.pop('incr')
        else:
            self.incr = incr
        self.labels = labels
        self.db_id = db_id

    @property
    def origin(self):
        return "{}_{}".format(self.edge_type, self.edge_value)
