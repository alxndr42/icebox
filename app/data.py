from enum import Enum


class JobStatus(Enum):
    """Status codes for long-running jobs."""

    running = 0
    success = 1
    failure = 2


class Source():
    """Local information on a stored source."""

    def __init__(self):
        self.name = None
        self.type = None
        self.sha256 = None
        self.data_key = None
        self.meta_key = None

    def __eq__(self, source2):
        """Comparison method."""
        if not isinstance(source2, Source):
            return False
        if self.name != source2.name:
            return False
        if self.type != source2.type:
            return False
        if self.sha256 != source2.sha256:
            return False
        if self.data_key != source2.data_key:
            return False
        if self.meta_key != source2.meta_key:
            return False
        return True
