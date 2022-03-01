import hashlib

import pytest

from icebox.box import SQLite
from icebox.data import Source


class TestSQLite():
    """Test the SQLite class."""

    def test_source_empty(self, datadir):
        """Test an empty database."""
        db = SQLite(datadir)
        assert db.load_source('test') is None
        sources = list(db.load_sources())
        assert len(sources) == 0
        db.delete_source('test')

    def test_source_save(self, datadir):
        """Test saving (and loading) a Source."""
        db = SQLite(datadir)
        source = FakeSource('test')
        db.save_source(source)
        assert db.load_source('test') is not None
        db = SQLite(datadir)  # load from a second instance
        source2 = db.load_source('test')
        assert source == source2

    def test_source_save_twice(self, datadir):
        """Test saving a Source twice."""
        db = SQLite(datadir)
        source = FakeSource('test')
        db.save_source(source)
        with pytest.raises(Exception) as e:
            db.save_source(source)

    def test_source_delete(self, datadir):
        """Test deleting a Source."""
        db = SQLite(datadir)
        source = FakeSource('test')
        db.save_source(source)
        assert db.load_source('test') is not None
        db.delete_source('test')
        assert db.load_source('test') is None

    def test_job_empty(self, datadir):
        """Test an empty database."""
        db = SQLite(datadir)
        assert db.load_job('test') is None
        db.delete_job('test')

    def test_job_save(self, datadir):
        """Test saving (and loading) a job."""
        db = SQLite(datadir)
        db.save_job('test', 'foo')
        assert db.load_job('test') is not None
        db = SQLite(datadir)  # load from a second instance
        job2 = db.load_job('test')
        assert job2 == 'foo'

    def test_job_save_twice(self, datadir):
        """Test saving a job twice."""
        db = SQLite(datadir)
        db.save_job('test', 'foo')
        with pytest.raises(Exception) as e:
            db.save_job('test', 'bar')

    def test_job_delete(self, datadir):
        """Test deleting a job."""
        db = SQLite(datadir)
        db.save_job('test', 'foo')
        assert db.load_job('test') is not None
        db.delete_job('test')
        assert db.load_job('test') is None


class FakeSource(Source):
    """Source for testing."""

    def __init__(self, name):
        super().__init__()
        self.name = name
        name_hash = hashlib.sha256(name.encode()).hexdigest()
        self.data_key = 'data:' + name_hash
        self.meta_key = 'meta:' + name_hash
