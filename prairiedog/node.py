class Node:
    """
    Defines a set structure for creating nodes
    """
    def __init__(self, value: str, node_type: str = 'n', db_id: int = None,
                 labels: dict = None):
        self.value = value
        self.node_type = node_type
        self.db_id = db_id
        self.labels = labels

    def __str__(self):
        return "{}".format(vars(self))
