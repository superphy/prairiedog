import abc


class GRef(metaclass=abc.ABCMeta):
    """
    Base class for graph reference classes.
    """
    @staticmethod
    def _upsert_map(d: dict, value: str, prev_value: int = 0) -> int:
        if value not in d:
            # This starts the MIC label at 1 or prev_value + 1
            d[value] = len(d) + prev_value + 1
        return d[value]
