from shutil import rmtree
import sqlite3
import time
import uuid

import yaml

from icepack import IcepackReader, create_archive, extract_archive
from icepack.helper import File, SSH, Zip
from icepack.model import Compression
from icepack.meta import SECRET_KEY

from icebox import SECRET_KEY
from icebox.backend import get_backend
from icebox.data import JobStatus, Source


DATA_SUFFIX = '.data'
META_SUFFIX = '.meta'

SQL_SCHEMA_VERSION = '3'
SQL_CREATE_SOURCES = '''CREATE TABLE IF NOT EXISTS sources (
                        name text UNIQUE NOT NULL,
                        comment text,
                        size int NOT NULL,
                        data_key text NOT NULL,
                        meta_key text NOT NULL)
                        '''
SQL_CREATE_JOBS = '''CREATE TABLE IF NOT EXISTS jobs (
                     name text UNIQUE NOT NULL,
                     key text NOT NULL)
                     '''
SQL_CREATE_SETTINGS = '''CREATE TABLE IF NOT EXISTS settings (
                         key text UNIQUE NOT NULL,
                         value text)
                         '''

INVENTORY_JOB = '::inventory::'


class Box():
    """Box configuration and mappings."""

    def __init__(self, path):
        self.path = path
        self.config_file = self.path / 'config.yml'
        if self.exists():
            with open(self.config_file, 'r') as f:
                self.config = yaml.safe_load(f)
            self._db = SQLite(path)
        else:
            self.config = {}

    def exists(self):
        """Check if a configuration file for this box exists."""
        return self.config_file.exists()

    def init(self, log=lambda msg: None):
        """Initialize this box on creation."""
        if self.exists():
            raise Exception('Box already initialized.')
        self.path.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._db = SQLite(self.path)
        if not (self.path / SECRET_KEY).exists():
            log('- Generating encryption keys.')
            SSH.keygen(self.path)
        backend = self._backend()
        backend.box_init()
        with open(self.config_file, 'w') as f:
            yaml.safe_dump(self.config, f, default_flow_style=False)

    def store(
            self,
            src_path,
            comment=None,
            compression=Compression.GZ,
            mode=False,
            mtime=False,
            log=lambda msg: None):
        """Encrypt the given source and store in backend."""
        if not self.exists():
            raise Exception('Box not found.')
        backend = self._backend()
        temp_path = File.mktemp(directory=True)
        data_path = temp_path / (src_path.name + '.zip')
        meta_path = temp_path / (src_path.name + META_SUFFIX)
        try:
            log('- Creating archive.')
            create_archive(
                src_path,
                data_path,
                self.path,
                comment=comment,
                compression=compression,
                mode=mode,
                mtime=mtime)
            _export_metadata(data_path, meta_path)
            data_name, meta_name = _backend_names()
            source = Source(src_path.name)
            log('- Transferring to backend.')
            source.comment = comment
            source.size = data_path.stat().st_size
            source.data_key = backend.store_data(data_path, data_name)
            source.meta_key = backend.store_meta(meta_path, meta_name)
            self._db.save_source(source)
        finally:
            rmtree(temp_path, ignore_errors=True)

    def retrieve(
            self,
            name,
            dst_path,
            backend_options,
            mode=False,
            mtime=False,
            log=lambda msg: None):
        """Retrieve source from the backend and decrypt."""
        if not self.exists():
            raise Exception('Box not found.')
        backend = self._backend()
        data_path = None
        try:
            source = self._db.load_source(name)
            # Get existing retrieval job or start a new one
            job = self._db.load_job(name)
            if job is None:
                log('- Initiating transfer from backend.')
                job = backend.retrieve_init(source.data_key, backend_options)
                self._db.save_job(name, job)
            # Wait until job is done
            status = backend.retrieve_status(job)
            if status == JobStatus.running:
                log('- Transfer from backend pending.')
            while status == JobStatus.running:
                time.sleep(60)
                status = backend.retrieve_status(job)
            if status == JobStatus.failure:
                self._db.delete_job(name)
                raise Exception('Transfer from backend failed.')
            # Download the data file
            log('- Transferring from backend.')
            data_path = backend.retrieve_finish(job)
            self._db.delete_job(name)
            # Decrypt original source
            log('- Extracting archive.')
            extract_archive(
                data_path,
                dst_path,
                self.path,
                mode=mode,
                mtime=mtime)
        finally:
            if data_path:
                data_path.unlink(missing_ok=True)

    def contains(self, source):
        """Return True if the source name exists in this box."""
        if not self.exists():
            raise Exception('Box not found.')
        return self._db.load_source(source) is not None

    def delete(self, source):
        """Delete encrypted data and metadata in the backend."""
        if not self.exists():
            raise Exception('Box not found.')
        backend = self._backend()
        src = self._db.load_source(source)
        backend.delete(src.data_key)
        backend.delete(src.meta_key)
        self._db.delete_source(source)

    def sources(self):
        """Return information about known sources."""
        if not self.exists():
            raise Exception('Box not found.')
        return self._db.load_sources()

    def refresh(self, backend_options, log=lambda msg: None):
        """Refresh local information from the backend."""
        if not self.exists():
            raise Exception('Box not found.')
        backend = self._backend()
        inventory_job = self._db.load_job(INVENTORY_JOB)
        if inventory_job is None:
            log('- Initiating inventory retrieval.')
            inventory_job = backend.inventory_init()
            self._db.save_job(INVENTORY_JOB, inventory_job)
        status = backend.inventory_status(inventory_job)
        if status == JobStatus.running:
            log('- Inventory retrieval pending.')
        while status == JobStatus.running:
            time.sleep(60)
            status = backend.inventory_status(inventory_job)
        if status == JobStatus.failure:
            self._db.delete_job(INVENTORY_JOB)
            raise Exception('Inventory retrieval failed.')
        log('- Inventory retrieval finished.')

        sources = self.sources()
        inventory = backend.inventory_finish(inventory_job)
        check = InventoryCheck(sources, inventory)

        jobs = {}
        for meta, data in check.importable.items():
            job = self._db.load_job(meta.key)
            if job is None:
                log(f'- Initiating metadata retrieval: {meta.key}')
                job = backend.retrieve_init(meta.key, backend_options)
                self._db.save_job(meta.key, job)
            jobs[job] = meta
        while jobs:
            finished = []
            for job, meta in jobs.items():
                status = backend.retrieve_status(job)
                if status != JobStatus.running:
                    self._db.delete_job(meta.key)
                if status == JobStatus.failure:
                    raise Exception(f'Metadata retrieval failed: {meta.key})')
                elif status == JobStatus.success:
                    meta_path = backend.retrieve_finish(job)
                    with IcepackReader(meta_path, self.path) as archive:
                        name = archive.metadata.archive_name
                    if name.endswith('.zip'):
                        name = name[:-4]
                    data = check.importable[meta]
                    src = Source(name)
                    src.comment = archive.metadata.comment
                    src.size = data.size
                    src.data_key = data.key
                    src.meta_key = meta.key
                    meta_path.unlink()
                    if not self.contains(src.name):
                        self._db.save_source(src)
                        log(f'- Added {src.name} = {meta.key}')
                    else:
                        log(f'- Ignoring duplicate {src.name} = {meta.key}')
                    finished.append(job)
            jobs = {j: k for j, k in jobs.items() if j not in finished}
            if jobs:
                log(f'- Metadata retrievals pending: {len(jobs)}')
                time.sleep(60)
        self._db.delete_job(INVENTORY_JOB)

        if check.broken_sources:
            log('- Sources with missing backend files:')
            for name, missing in check.broken_sources.items():
                log(f'  {name}: {" / ".join(missing)}')

        if check.broken_backend:
            log('- Dangling backend files:')
            for f in check.broken_backend:
                log(f'  {f.key}')

    def _backend(self):
        """Return a backend instance for this box."""
        return get_backend(self.config['backend'], self.path, self.config)


class InventoryCheck():
    """Find broken sources and broken/importable backend files."""

    def __init__(self, sources, inventory):
        backend_keys = {f.key: f for f in inventory}
        # Check sources
        self.broken_sources = {}
        for s in sources:
            data = backend_keys.pop(s.data_key, None)
            meta = backend_keys.pop(s.meta_key, None)
            if data and meta:
                continue
            missing = []
            if not data:
                missing.append(s.data_key)
            if not meta:
                missing.append(s.meta_key)
            self.broken_sources[s.name] = missing
        # Check backend files
        self.importable = {}
        self.broken_backend = []
        inventory = backend_keys.values()
        backend_data = self._map_by_name_suffix(inventory, DATA_SUFFIX)
        backend_meta = self._map_by_name_suffix(inventory, META_SUFFIX)
        for name, data in backend_data.items():
            meta = backend_meta.pop(_sibling(name), None)
            if meta:
                self.importable[meta] = data
            else:
                self.broken_backend.append(data)
        self.broken_backend.extend(backend_meta.values())

    @staticmethod
    def _map_by_name_suffix(inventory, suffix):
        """Map name to BackendFile by name suffix."""
        return {f.name: f for f in inventory if f.name.endswith(suffix)}


class SQLite():
    """SQLite wrapper for source and job state."""

    def __init__(self, path):
        db_path = path.joinpath('box.db')
        self._conn = sqlite3.connect(str(db_path))
        self._ensure_tables()

    def load_source(self, name):
        """Return a Source by name."""
        stmt = '''SELECT name, comment, size, data_key, meta_key
                  FROM sources
                  WHERE name=?
                  '''
        c = self._conn.execute(stmt, (name,))
        r = c.fetchone()
        c.close()
        if r is not None:
            return self._to_source(r)
        else:
            return None

    def load_sources(self):
        """Return an iterable for all Sources."""
        stmt = '''SELECT name, comment, size, data_key, meta_key
                  FROM sources
                  ORDER BY name COLLATE NOCASE
                  '''
        c = self._conn.execute(stmt)

        def generator():
            for r in c:
                yield self._to_source(r)
            c.close()

        return generator()

    def save_source(self, source):
        """Save a Source."""
        stmt = '''INSERT INTO sources
                  (name, comment, size, data_key, meta_key)
                  VALUES (?, ?, ?, ?, ?)
                  '''
        values = (
            source.name,
            source.comment,
            source.size,
            source.data_key,
            source.meta_key
        )
        with self._conn:
            self._conn.execute(stmt, values)

    def delete_source(self, name):
        """Delete a Source by name."""
        stmt = 'DELETE FROM sources WHERE name=?'
        with self._conn:
            self._conn.execute(stmt, (name,))

    def load_job(self, name):
        """Return the job key for the given name."""
        stmt = 'SELECT key FROM jobs WHERE name=?'
        c = self._conn.execute(stmt, (name,))
        r = c.fetchone()
        c.close()
        if r is not None:
            return r[0]
        else:
            return None

    def save_job(self, name, key):
        """Save the job key for the given name."""
        stmt = 'INSERT INTO jobs (name, key) VALUES (?, ?)'
        values = (name, key)
        with self._conn:
            self._conn.execute(stmt, values)

    def delete_job(self, name):
        """Delete the job key for the given name."""
        stmt = 'DELETE FROM jobs WHERE name=?'
        with self._conn:
            self._conn.execute(stmt, (name,))

    def _ensure_tables(self):
        """Ensure all tables exist and are up to date."""
        with self._conn:
            self._conn.execute(SQL_CREATE_SETTINGS)
        stmt = 'SELECT value FROM settings WHERE key="schema"'
        c = self._conn.execute(stmt)
        r = c.fetchone()
        c.close()
        if r is None:
            schema = None
        else:
            schema = r[0]
        if schema is None:
            self._create_tables()
        elif schema == SQL_SCHEMA_VERSION:
            pass
        else:
            raise Exception('Unsupported schema version: ' + str(schema))

    def _create_tables(self):
        """Create all tables from scratch."""
        schema = 'INSERT INTO settings VALUES ("schema", ?)'
        with self._conn:
            self._conn.execute(SQL_CREATE_SOURCES)
            self._conn.execute(SQL_CREATE_JOBS)
            self._conn.execute(schema, SQL_SCHEMA_VERSION)

    @staticmethod
    def _to_source(row):
        """Return a Source for the given row."""
        source = Source()
        source.name = row[0]
        source.comment = row[1]
        source.size = row[2]
        source.data_key = row[3]
        source.meta_key = row[4]
        return source


def _backend_names():
    """Return a tuple of names for encrypted data and metadata files."""
    basename = str(uuid.uuid4())
    data_name = basename + DATA_SUFFIX
    meta_name = basename + META_SUFFIX
    return data_name, meta_name


def _export_metadata(src_path, dst_path):
    """Export metadata from src_path to dst_path."""
    with Zip(src_path) as src:
        meta_path, sig_path = src.extract_metadata()
        with Zip(dst_path, 'w') as dst:
            dst.add_metadata(meta_path, sig_path)


def _sibling(name):
    """Return the metadata name for a data file and vice versa."""
    if name.endswith(DATA_SUFFIX):
        return name[:-len(DATA_SUFFIX)] + META_SUFFIX
    elif name.endswith(META_SUFFIX):
        return name[:-len(META_SUFFIX)] + DATA_SUFFIX
    else:
        raise Exception(f'Invalid name: {name}')
