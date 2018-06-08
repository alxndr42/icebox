import io
import shutil

import gnupg
import yaml

from app.util import File


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
            src_path = File.tar(src_path)

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

        return data_path, meta_path

    def decrypt(self, data_path, meta_path, dst_path):
        """Recreate the source from encrypted data and metadata."""
        try:
            with open(meta_path, 'rb') as src:
                metayaml = self._decrypt_to_string(src)
        except Exception as e:
            msg = 'Metadata decryption failed. ({})'.format(e)
            raise Exception(msg)

        metadata = yaml.safe_load(metayaml)
        source_name = metadata['name']
        source_type = metadata['type']
        old_sha256 = metadata['sha256']
        new_sha256 = File.sha256(data_path)
        if old_sha256 != new_sha256:
            raise Exception('Source checksum failed.')

        if source_type == 'file':
            source_path = dst_path.joinpath(source_name)
        elif source_type == 'directory/tar':
            source_path = File.mktemp()
        else:
            raise Exception('Unsupported type: ' + str(source_type))

        try:
            with open(data_path, 'rb') as src:
                self._decrypt_to_file(src, source_path)
        except Exception as e:
            msg = 'Source decryption failed. ({})'.format(e)
            raise Exception(msg)

        if source_type == 'directory/tar':
            shutil.unpack_archive(source_path, dst_path, 'tar')
            source_path.unlink()

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
