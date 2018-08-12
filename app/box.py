import logging
import sqlite3
import time
import uuid

import yaml

from app.backend import get_backend
from app.data import JobStatus, Source


DATA_SUFFIX = '.data'
META_SUFFIX = '.meta'

LOG = logging.getLogger(__name__)

SQL_SCHEMA_VERSION = '1'
SQL_CREATE_SOURCES = '''CREATE TABLE IF NOT EXISTS sources (
                        name text UNIQUE NOT NULL,
                        type text NOT NULL,
                        data_hash text NOT NULL,
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
        self.path.mkdir(mode=0o770, parents=True, exist_ok=True)
        self.config_file = self.path.joinpath('config.yml')
        if self.config_file.exists():
            with open(self.config_file, 'r') as f:
                self.config = yaml.safe_load(f)
        else:
            self.config = {}
        self._db = SQLite(path)
        self._gpg = None

    def exists(self):
        """Check if a configuration file for this box exists."""
        return self.config_file.exists()

    def init(self):
        """Initialize this box on creation."""
        if self.exists():
            raise Exception('Box already initialized.')

        backend = self.backend()
        backend.box_init()
        self._save_config()

    @property
    def gpg(self):
        """Get the GPG instance for this box."""
        if self._gpg is not None:
            return self._gpg
        else:
            raise Exception('No GPG instance.')

    @gpg.setter
    def gpg(self, value):
        """Set the GPG instance for this box."""
        assert value is not None
        self._gpg = value

    @property
    def key(self):
        """Get the GnuPG key ID for this box."""
        if 'key-id' in self.config:
            return self.config['key-id']
        else:
            raise Exception('No key ID.')

    @key.setter
    def key(self, value):
        """Set the GnuPG key ID for this box."""
        assert value is not None
        self.config['key-id'] = value

    def _save_config(self):
        """Save the configuration to file."""
        with open(self.config_file, 'w') as f:
            yaml.safe_dump(self.config, f, default_flow_style=False)

    def store(self, src_path):
        """Encrypt the given source and store in backend."""
        data_path, meta_path = self.gpg.encrypt(src_path, self.key)
        data_name, meta_name = _backend_names()
        backend = self.backend()
        try:
            source = self.gpg.decrypt_meta(meta_path)
            LOG.debug('Storing %s', source.name)
            source.data_key = backend.store(data_path, data_name)
            source.meta_key = backend.store(meta_path, meta_name)
            LOG.debug('Stored %s', source.name)
            self._db.save_source(source)
        except Exception as e:
            msg = 'Storage operation failed. ({})'.format(e)
            raise Exception(msg)
        finally:
            if data_path and data_path.exists():
                data_path.unlink()
            if meta_path and meta_path.exists():
                meta_path.unlink()

    def retrieve(self, name, dst_path, backend_options):
        """Retrieve source from the backend and decrypt."""
        data_path = None
        backend = self.backend()
        try:
            source = self._db.load_source(name)
            # Get existing retrieval job or start a new one
            key = self._db.load_job(name)
            if key is None:
                key = backend.retrieve_init(source.data_key, backend_options)
                self._db.save_job(name, key)

            # Wait until job is done
            status = backend.retrieve_status(key)
            while status == JobStatus.running:
                LOG.debug('Retrieve pending for %s', name)
                time.sleep(60)
                status = backend.retrieve_status(key)
            self._db.delete_job(name)
            if status == JobStatus.failure:
                raise Exception('Retrieval job failed.')

            # Download the data file
            LOG.debug('Retrieving %s', name)
            data_path = backend.retrieve_finish(key)
            LOG.debug('Retrieved %s', name)

            # Decrypt original source
            self.gpg.decrypt_data(source, data_path, dst_path)
        except Exception as e:
            msg = 'Retrieval operation failed. ({})'.format(e)
            raise Exception(msg)
        finally:
            if data_path and data_path.exists():
                data_path.unlink()

    def contains(self, source):
        """Return True if the source name exists in this box."""
        return self._db.load_source(source) is not None

    def delete(self, source):
        """Delete encrypted data and metadata in the backend."""
        backend = self.backend()
        try:
            src = self._db.load_source(source)
            backend.delete(src.data_key)
            backend.delete(src.meta_key)
            self._db.delete_source(source)
        except Exception as e:
            msg = 'Delete operation failed. ({})'.format(e)
            raise Exception(msg)

    def sources(self):
        """Return information about known sources."""
        return self._db.load_sources()

    def refresh(self, backend_options):
        """Refresh local information from the backend."""
        backend = self.backend()
        inventory_job = self._db.load_job(INVENTORY_JOB)
        if inventory_job is None:
            LOG.debug('Initiating inventory job')
            inventory_job = backend.inventory_init()
            self._db.save_job(INVENTORY_JOB, inventory_job)
        status = backend.inventory_status(inventory_job)
        while status == JobStatus.running:
            LOG.debug('Inventory pending')
            time.sleep(60)
            status = backend.inventory_status(inventory_job)
        if status == JobStatus.failure:
            self._db.delete_job(INVENTORY_JOB)
            raise Exception('Inventory job failed.')

        sources = self.sources()
        inventory = backend.inventory_finish(inventory_job)
        verifier = KeyVerifier(sources, inventory)

        duplicates = []
        jobs = {}
        for meta_key in verifier.unknowns:
            job = self._db.load_job(meta_key)
            if job is None:
                LOG.debug('Initiating metadata retrieval')
                job = backend.retrieve_init(meta_key, backend_options)
                self._db.save_job(meta_key, job)
            jobs[job] = meta_key
        while jobs:
            finished = []
            for job, meta_key in jobs.items():
                status = backend.retrieve_status(job)
                if status != JobStatus.running:
                    self._db.delete_job(meta_key)
                if status == JobStatus.failure:
                    raise Exception('Metadata retrieval failed.')
                elif status == JobStatus.success:
                    meta_path = backend.retrieve_finish(job)
                    src = self.gpg.decrypt_meta(meta_path)
                    src.data_key = verifier.unknowns[meta_key]
                    src.meta_key = meta_key
                    meta_path.unlink()
                    if not self.contains(src.name):
                        self._db.save_source(src)
                        LOG.debug('Added %s', src.name)
                    else:
                        duplicates.append(src)
                    finished.append(job)
            jobs = {j: k for j, k in jobs.items() if j not in finished}
            if jobs:
                LOG.debug('Metadata retrievals pending: %s', len(jobs))
                time.sleep(60)
        self._db.delete_job(INVENTORY_JOB)
        return duplicates, verifier.backend_singles

    def backend(self):
        """Return a backend instance for this box."""
        return get_backend(self.config['backend'], self.path, self.config)


class KeyVerifier():
    """Verify local and backend information."""

    def __init__(self, sources, inventory):
        local_keys = {}
        for s in sources:
            local_keys[s.data_key] = s.name
            local_keys[s.meta_key] = s.name
        backend_data = self._filter_key_suffix(inventory, DATA_SUFFIX)
        backend_meta = self._filter_key_suffix(inventory, META_SUFFIX)
        # Find unmatched backend keys
        singles_data = self._singles(backend_data, backend_meta)
        singles_meta = self._singles(backend_meta, backend_data)
        self.backend_singles = {}
        self.backend_singles.update(singles_data)
        self.backend_singles.update(singles_meta)
        for name in singles_data.keys():
            del backend_data[name]
        for name in singles_meta.keys():
            del backend_meta[name]
        # Find unknown backend metadata keys and their data siblings
        meta = {
            n: k for n, k in backend_meta.items() if k not in local_keys}
        self.unknowns = {
            k: backend_data[_sibling_name(n)] for n, k in meta.items()}

    @staticmethod
    def _filter_key_suffix(dict_, suffix):
        """Filter dict_ by key suffix."""
        return {k: v for k, v in dict_.items() if k.endswith(suffix)}

    @staticmethod
    def _singles(dictA, dictB):
        """Return items from dictA that have no sibling in dictB."""
        def single(name):
            return _sibling_name(name) not in dictB
        return {k: v for k, v in dictA.items() if single(k)}


class SQLite():
    """SQLite wrapper for source and job state."""

    def __init__(self, path):
        db_path = path.joinpath('box.db')
        self._conn = sqlite3.connect(str(db_path))
        self._ensure_tables()

    def load_source(self, name):
        """Return a Source by name."""
        stmt = '''SELECT name, type, data_hash, data_key, meta_key
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
        stmt = '''SELECT name, type, data_hash, data_key, meta_key
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
                  (name, type, data_hash, data_key, meta_key)
                  VALUES (?, ?, ?, ?, ?)
                  '''
        data_hash = 'sha256:' + source.sha256
        values = (
            source.name,
            source.type,
            data_hash,
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
        source.type = row[1]
        data_hash = row[2]
        if data_hash.startswith('sha256:'):
            source.sha256 = data_hash[7:]
        else:
            raise Exception('Unsupported data hash: ' + str(data_hash))
        source.data_key = row[3]
        source.meta_key = row[4]
        return source


def get_box(base_path, box_name):
    """Return a Box instance for the given base directory and name."""
    box_path = base_path.joinpath(box_name)
    box = Box(box_path)
    return box


def _backend_names():
    """Return a tuple of names for encrypted data and metadata files."""
    basename = str(uuid.uuid4())
    data_name = basename + DATA_SUFFIX
    meta_name = basename + META_SUFFIX
    return data_name, meta_name


def _sibling_name(name):
    """Return the metadata name for a data file and vice versa."""
    if name.endswith(DATA_SUFFIX):
        return name[:-len(DATA_SUFFIX)] + META_SUFFIX
    elif name.endswith(META_SUFFIX):
        return name[:-len(META_SUFFIX)] + DATA_SUFFIX
    else:
        raise Exception('Invalid name: ' + str(name))
