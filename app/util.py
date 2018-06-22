from enum import Enum
import hashlib
import io
import os
from pathlib import Path
import shutil
import sys
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
            tmpfile = File.mktemp()
            tmpfile.unlink()
            base = str(tmpfile)
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


class OffsetRangeWrapper():
    """Expose only part of a stream for reading.

       Given a start and end offset, allows reading bytes from start offset to
       end offset - 1 from the wrapped stream. The seek and tell methods use a
       virtual offset ranging from 0 (start offset) to maximum (end offset -
       start offset).
       """
    def __init__(self, stream, start_offset, end_offset):
        assert 0 <= start_offset <= end_offset
        self.stream = stream
        self.start_offset = start_offset
        self.end_offset = end_offset
        stream.seek(start_offset)

    def read(self, size=-1):
        """Read bytes from the offset range."""
        if size is None or size < 0:
            size = sys.maxsize
        current = self.stream.tell()
        if current < self.end_offset:
            actual = min(size, self.end_offset - current)
            return self.stream.read(actual)
        else:
            return bytes()

    def seek(self, offset, whence=io.SEEK_SET):
        """Seek to the given virtual offset in the offset range."""
        if whence == io.SEEK_CUR:
            absolute = self.stream.tell() + offset
        elif whence == io.SEEK_END:
            absolute = self.end_offset + offset
        else:
            absolute = self.start_offset + offset
        actual = min(absolute, self.end_offset)
        actual = max(actual, self.start_offset)
        return self.stream.seek(actual)

    def tell(self):
        """Return the virtual offset in the offset range."""
        current = self.stream.tell()
        virtual = current - self.start_offset
        return virtual
