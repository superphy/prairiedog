import typing


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
        return "prairiedog.node.Node with vars {}".format(vars(self))


def concat_values(nodes: typing.Tuple[Node], additional: int = 1) -> str:
    """
    Concatenate value strings in Nodes. Used to reconstruct Kmers.
    :param additional:
    :param nodes:
    :return:
    """
    if len(nodes) == 0:
        return ""
    s = nodes[0].value
    for i in range(1, len(nodes)):
        node = nodes[i]
        # For example: "ABC", "BCD" has additional = 1, so we take "ABC" += "D"
        suffix = node.value[len(node.value)-additional:]
        s += suffix
    return s
