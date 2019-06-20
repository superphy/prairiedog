from prairiedog.dbm_map import upsert


class Edge:
    """
    Defines a set structure for creating edges
    """

    def __init__(self, src: str, tgt: str, edge_type: str = 'e',
                 edge_value: int = -1, labels: dict = None, db_id=None):
        self.src = src
        self.tgt = tgt
        # Note: LemonGraph uses the type + value as str when adding, but
        # returns ordinal IDs when called
        try:
            # Check if this is some pre-converted int from the database
            int(edge_type)
            self.edge_type = edge_type
        except ValueError:
            # We were passed an actual string, so index it and convert
            self.edge_type = str(upsert(edge_type))
        if edge_value == -1 and labels is not None and 'incr' in labels:
            self.edge_value = labels['incr']
            labels.pop('incr')
        else:
            self.edge_value = edge_value
        if not isinstance(self.edge_value, int):
            self.edge_value = int(self.edge_value)
        self.labels = labels
        self.db_id = db_id

    @property
    def origin(self) -> str:
        return self.edge_type

    def __str__(self):
        return "prairiedog.edge.Edge with vars {}".format(vars(self))
    #
    # def __eq__(self, other):
    #     if not isinstance(other, Edge):
    #         return False
    #     else:
    #         # We don't just test db_id because it might not be present unless
    #         # queried from the database
    #         return self.edge_type == other.edge_type \
    #                and self.edge_value == other.edge_value \
    #                and self.incr == other.incr
