from enum import Enum


class BackendFile():

    def __init__(self, key, name, size):
        self.key = key
        self.name = name
        self.size = size

    def __repr__(self):
        return f'<{self.name}|{self.key}|{self.size}>'


class JobStatus(Enum):
    """Status codes for long-running jobs."""

    running = 0
    success = 1
    failure = 2


class Source():
    """Local information on a stored source."""

    def __init__(self, name=None):
        self.name = name
        self.comment = None
        self.size = None
        self.data_key = None
        self.meta_key = None

    def __eq__(self, other):
        """Comparison method."""
        if not isinstance(other, Source):
            return False
        if self.name != other.name:
            return False
        if self.comment != other.comment:
            return False
        if self.size != other.size:
            return False
        if self.data_key != other.data_key:
            return False
        if self.meta_key != other.meta_key:
            return False
        return True
