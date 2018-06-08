from enum import Enum
import hashlib
import os
from pathlib import Path
import shutil
import tempfile

from app import NAME


class File():
    """Various file operations."""

    @staticmethod
    def mktemp():
        """Create a temporary file, return its Path."""
        fd, tmpfile = tempfile.mkstemp(prefix=NAME)
        os.close(fd)
        return Path(tmpfile)

    @staticmethod
    def sha256(path):
        """Return the SHA-256 digest of the given file."""
        d = hashlib.sha256()
        with open(path, 'rb') as src:
            while True:
                chunk = src.read(65536)
                if not chunk:
                    break
                d.update(chunk)
        return d.hexdigest()

    @staticmethod
    def tar(path):
        """Create a tar-file from the given Path, return its Path."""
        try:
            base = str(File.mktemp())
            tar = shutil.make_archive(base, 'tar', path.parent, path.name)
            return Path(tar)
        except Exception as e:
            msg = 'Tar operation failed. ({})'.format(e)
            raise Exception(msg)


class JobStatus(Enum):
    """Status codes for long-running jobs."""

    running = 0
    success = 1
    failure = 2
