import logging
import time
import uuid

import yaml

from app.backend import get_backend
from app.util import JobStatus, Source


DATA_SUFFIX = '.data'
META_SUFFIX = '.meta'

LOG = logging.getLogger(__name__)


class Box():
    """Box configuration and mappings."""

    def __init__(self, path):
        self.path = path
        self.config_file = self.path.joinpath('config.yml')
        if self.config_file.exists():
            with open(self.config_file, 'r') as f:
                self.config = yaml.safe_load(f)
        else:
            self.config = {}
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
        self.path.mkdir(mode=0o770, parents=True, exist_ok=True)
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
            self._save_source(source)
        except Exception as e:
            msg = 'Storage operation failed. ({})'.format(e)
            raise Exception(msg)
        finally:
            if data_path and data_path.exists():
                data_path.unlink()
            if meta_path and meta_path.exists():
                meta_path.unlink()

    def retrieve(self, source, dst_path, backend_options):
        """Retrieve source from the backend and decrypt."""
        data_path = None
        meta_path = None
        backend = self.backend()
        try:
            # Get existing jobs or start new ones
            if self._has_retrieval_jobs(source):
                data_job, meta_job = self._get_retrieval_jobs(source)
            else:
                src = self._load_source(source)
                data_job = backend.retrieve_init(src.data_key, backend_options)
                meta_job = backend.retrieve_init(src.meta_key, backend_options)
                self._set_retrieval_jobs(source, data_job, meta_job)

            # Wait until jobs are done
            status = backend.retrieve_status(data_job, meta_job)
            while status == JobStatus.running:
                LOG.debug('Retrieve pending for %s', source)
                time.sleep(60)
                status = backend.retrieve_status(data_job, meta_job)
            self._clear_retrieval_jobs(source)
            if status == JobStatus.failure:
                raise Exception('Retrieval job failed.')

            # Download the files
            LOG.debug('Retrieving %s', source)
            data_path = backend.retrieve_finish(data_job)
            meta_path = backend.retrieve_finish(meta_job)
            LOG.debug('Retrieved %s', source)

            # Decrypt original source
            self.gpg.decrypt(data_path, meta_path, dst_path)
        except Exception as e:
            msg = 'Retrieval operation failed. ({})'.format(e)
            raise Exception(msg)
        finally:
            if data_path and data_path.exists():
                data_path.unlink()
            if meta_path and meta_path.exists():
                meta_path.unlink()

    def contains(self, source):
        """Return True if the source name exists in this box."""
        key_path = self.path.joinpath('retrieval-keys')
        key_file = key_path.joinpath(source)
        return key_file.exists()

    def _load_source(self, source):
        """Load local source information."""
        key_path = self.path.joinpath('retrieval-keys')
        key_file = key_path.joinpath(source)
        if key_file.exists():
            with open(key_file, 'r') as f:
                keydict = yaml.safe_load(f)
            src = Source()
            src.name = source
            src.data_key = keydict['data-key']
            src.meta_key = keydict['meta-key']
            return src
        else:
            raise Exception('Source not found.')

    def _save_source(self, source):
        """Save local source information."""
        key_path = self.path.joinpath('retrieval-keys')
        key_path.mkdir(mode=0o770, parents=True, exist_ok=True)
        key_file = key_path.joinpath(source.name)
        keydict = {
            'data-key': source.data_key,
            'meta-key': source.meta_key,
        }
        with open(key_file, 'w') as f:
            yaml.safe_dump(keydict, f, default_flow_style=False)

    def _delete_source(self, source):
        """Delete local source information."""
        key_path = self.path.joinpath('retrieval-keys')
        key_file = key_path.joinpath(source)
        if key_file.exists():
            key_file.unlink()

    def _has_retrieval_jobs(self, source):
        """Return True if retrieval jobs exist for the given source."""
        job_path = self.path.joinpath('retrieval-jobs')
        job_file = job_path.joinpath(source)
        return job_file.exists()

    def _get_retrieval_jobs(self, source):
        """Get retrieval jobs for the given source."""
        job_path = self.path.joinpath('retrieval-jobs')
        job_file = job_path.joinpath(source)
        if job_file.exists():
            with open(job_file, 'r') as f:
                jobdict = yaml.safe_load(f)
            return jobdict.get('data-job'), jobdict.get('meta-job')
        else:
            return None, None

    def _set_retrieval_jobs(self, source, data_job, meta_job):
        """Set retrieval jobs for the given source."""
        job_path = self.path.joinpath('retrieval-jobs')
        job_path.mkdir(mode=0o770, parents=True, exist_ok=True)
        job_file = job_path.joinpath(source)
        jobdict = {
            'data-job': data_job,
            'meta-job': meta_job,
        }
        with open(job_file, 'w') as f:
            yaml.safe_dump(jobdict, f, default_flow_style=False)

    def _clear_retrieval_jobs(self, source):
        """Clear retrieval jobs for the given source."""
        job_path = self.path.joinpath('retrieval-jobs')
        job_file = job_path.joinpath(source)
        if job_file.exists():
            job_file.unlink()

    def delete(self, source):
        """Delete encrypted data and metadata in the backend."""
        backend = self.backend()
        try:
            src = self._load_source(source)
            backend.delete(src.data_key)
            backend.delete(src.meta_key)
            self._delete_source(source)
        except Exception as e:
            msg = 'Delete operation failed. ({})'.format(e)
            raise Exception(msg)

    def sources(self):
        """Return information about known sources."""
        key_path = self.path.joinpath('retrieval-keys')
        result = []
        if key_path.is_dir():
            for child in key_path.iterdir():
                src = Source()
                src.name = child.name
                result.append(src)
            result.sort(key=lambda s: s.name.lower())
        return result

    def backend(self):
        """Return a backend instance for this box."""
        return get_backend(self.config['backend'], self.path, self.config)


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
