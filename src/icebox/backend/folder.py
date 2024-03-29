from pathlib import Path
import shutil

from icepack.helper import File

from icebox.data import BackendFile, JobStatus


class Backend():
    """Backend for folder-backed boxes."""

    def __init__(self, box_path, box_config):
        self.box_path = box_path
        self.box_config = box_config
        self.folder_path = Path(box_config['folder_path'])

    def box_init(self):
        """Optional box initialization at creation time."""
        if not self.folder_path.exists():
            raise Exception(f'No such folder: {self.folder_path}')

    def store_data(self, src_path, name):
        """Store the data file as name, return a retrieval key."""
        return self._store(src_path, name)

    def store_meta(self, src_path, name):
        """Store the metadata file as name, return a retrieval key."""
        return self._store(src_path, name)

    def retrieve_init(self, retrieval_key, options):
        """Initiate a retrieval job, return the job key."""
        return retrieval_key

    def retrieve_status(self, job_key):
        """Return the JobStatus of the given job."""
        file_path = self.folder_path.joinpath(job_key)
        if file_path.exists():
            return JobStatus.success
        else:
            return JobStatus.failure

    def retrieve_finish(self, job_key):
        """Finish the job, return the temporary file's Path."""
        file_path = self.folder_path.joinpath(job_key)
        tmp_path = File.mktemp()
        shutil.copy(str(file_path), str(tmp_path))
        return tmp_path

    def delete(self, retrieval_key):
        """Delete the data for the given retrieval key."""
        file_path = self.folder_path.joinpath(retrieval_key)
        file_path.unlink(missing_ok=True)

    def inventory_init(self):
        """Initiate an inventory job, return the job key."""
        return 'inventory'

    def inventory_status(self, job_key):
        """Return the JobStatus of the given job."""
        return JobStatus.success

    def inventory_finish(self, job_key):
        """Return a list of BackendFiles."""
        result = []
        for c in self.folder_path.iterdir():
            if not c.is_file():
                continue
            result.append(BackendFile(c.name, c.name, c.stat().st_size))
        return result

    def _store(self, src_path, name):
        """Store the file at src_path as name, return a retrieval key."""
        dst_path = self.folder_path.joinpath(name)
        shutil.copy(str(src_path), str(dst_path))
        return name
