import io

import pytest

from app.util import OffsetRangeWrapper


@pytest.fixture
def wrapper():
    """Return a wrapped stream exposing b'bcde'."""
    s = io.BytesIO('abcdef'.encode())
    w = OffsetRangeWrapper(s, 1, 5)
    return w


class TestOffsetRangeWrapper():
    """Test the OffsetRangeWrapper."""

    def test_read_one(self, wrapper):
        """Read the first byte."""
        b = wrapper.read(1)
        assert b == b'b'

    def test_read_five(self, wrapper):
        """Read five bytes, expecting four."""
        b = wrapper.read(5)
        assert b == b'bcde'

    def test_read_all(self, wrapper):
        """Read unlimited bytes, expecting four."""
        b = wrapper.read()
        assert b == b'bcde'

    def test_read_end(self, wrapper):
        """Read at EOF."""
        b = wrapper.read()
        b = wrapper.read()
        assert not b

    def test_seek_and_tell(self, wrapper):
        """Seek to various positions and check the offset."""
        o = wrapper.tell()
        assert o == 0
        wrapper.seek(1)
        o = wrapper.tell()
        assert o == 1
        wrapper.seek(1, io.SEEK_CUR)
        o = wrapper.tell()
        assert o == 2
        wrapper.seek(-1, io.SEEK_END)
        o = wrapper.tell()
        assert o == 3
        wrapper.seek(0, io.SEEK_END)
        o = wrapper.tell()
        assert o == 4

    def test_seek_and_read(self, wrapper):
        """Seek to various positions and read."""
        wrapper.seek(1)
        b = wrapper.read(1)
        assert b == b'c'
        wrapper.seek(1, io.SEEK_CUR)
        b = wrapper.read(1)
        assert b == b'e'
        wrapper.seek(-1, io.SEEK_END)
        b = wrapper.read(1)
        assert b == b'e'
        wrapper.seek(0, io.SEEK_END)
        b = wrapper.read(1)
        assert not b
