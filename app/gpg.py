import io
import logging
import shutil

import gnupg
import yaml

from app.data import Source
from app.util import File


LOG = logging.getLogger(__name__)


class GPG():
    """GnuPG support class."""

    def __init__(self, gpg_path=None):
        if gpg_path is None or not gpg_path.is_dir():
            self.gpg = gnupg.GPG(use_agent=True)
        else:
            self.gpg = gnupg.GPG(use_agent=True, gnupghome=str(gpg_path))

    def valid_key_id(self, key_id):
        """Return True if a single keypair for the given ID exists."""
        keys = self.gpg.list_keys(True, keys=key_id)
        return len(keys) == 1

    def encrypt(self, src_path, key_id):
        """Encrypt the source as data and metadata files.

           Return a Path tuple of data and metadata file.
           """
        src_name = src_path.name
        if src_path.is_file():
            src_type = 'file'
        elif src_path.is_dir():
            src_type = 'directory/tar'
        else:
            raise Exception('Unsupported source type.')

        if src_type != 'file':
            LOG.debug('Creating tar file for %s', src_name)
            src_path = File.tar(src_path)
            LOG.debug('Created tar file for %s', src_name)

        LOG.debug('Encrypting %s', src_name)
        try:
            data_path = File.mktemp()
            with open(src_path, 'rb') as src:
                self._encrypt_to_file(src, data_path, key_id)
        except Exception as e:
            msg = 'Source encryption failed. ({})'.format(e)
            raise Exception(msg)
        finally:
            if src_type != 'file' and src_path.exists():
                src_path.unlink()

        sha256 = File.sha256(data_path)
        metadata = {
            'name': src_name,
            'type': src_type,
            'sha256': sha256,
        }
        metayaml = yaml.safe_dump(metadata, default_flow_style=False)
        try:
            src = io.BytesIO(metayaml.encode())
            meta_path = File.mktemp()
            self._encrypt_to_file(src, meta_path, key_id)
        except Exception as e:
            msg = 'Metadata encryption failed. ({})'.format(e)
            raise Exception(msg)
        LOG.debug('Encrypted %s', src_name)

        return data_path, meta_path

    def decrypt_data(self, source, data_path, dst_path):
        """Decrypt the given data file."""
        LOG.debug('Decrypting %s', source.name)
        sha256 = File.sha256(data_path)
        if source.sha256 != sha256:
            raise Exception('Source checksum failed.')

        if source.type == 'file':
            src_path = dst_path.joinpath(source.name)
        elif source.type == 'directory/tar':
            src_path = File.mktemp()
        else:
            raise Exception('Unsupported type: ' + str(source.type))

        try:
            with open(data_path, 'rb') as src:
                self._decrypt_to_file(src, src_path)
        except Exception as e:
            msg = 'Source decryption failed. ({})'.format(e)
            raise Exception(msg)
        LOG.debug('Decrypted %s', source.name)

        if source.type == 'directory/tar':
            LOG.debug('Unpacking tar file for %s', source.name)
            shutil.unpack_archive(src_path, dst_path, 'tar')
            LOG.debug('Unpacked tar file for %s', source.name)
            src_path.unlink()

    def decrypt_meta(self, meta_path):
        """Decrypt the given metadata file and return a Source."""
        try:
            with open(meta_path, 'rb') as src:
                metayaml = self._decrypt_to_string(src)
            metadata = yaml.safe_load(metayaml)
            source = Source()
            source.name = metadata['name']
            source.type = metadata['type']
            source.sha256 = metadata['sha256']
        except Exception as e:
            msg = 'Metadata decryption failed. ({})'.format(e)
            raise Exception(msg)
        return source

    def _encrypt_to_file(self, src, dst_path, key_id):
        """Encrypt and sign the given source."""
        key_fp = self._fingerprint(key_id)
        result = self.gpg.encrypt_file(
            src, key_fp, sign=key_fp, armor=False, output=str(dst_path))
        if not result.ok:
            raise Exception(result.status)

    def _fingerprint(self, key_id):
        """Return the fingerprint for the given key ID."""
        keys = self.gpg.list_keys(True, keys=key_id)
        if len(keys) == 0:
            raise Exception('Key not found: ' + str(key_id))
        if len(keys) > 1:
            raise Exception('Ambiguous key ID: ' + str(key_id))
        return keys[0]['fingerprint']

    def _decrypt_to_file(self, src, dst_path):
        """Decrypt and verify the given source, as a file."""
        result = self.gpg.decrypt_file(src, output=str(dst_path))
        self._verify_decrypt_result(result)

    def _decrypt_to_string(self, src):
        """Decrypt and verify the given source, as a string."""
        result = self.gpg.decrypt_file(src)
        self._verify_decrypt_result(result)
        return str(result)

    @staticmethod
    def _verify_decrypt_result(result):
        """Check for successful decryption and a trusted signature."""
        if not result.ok:
            raise Exception(result.status)
        if result.trust_level is None:
            raise Exception('Unsigned data.')
        if result.trust_level < result.TRUST_FULLY:
            raise Exception('Untrusted signature.')
