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
