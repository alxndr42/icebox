from pathlib import Path
import shutil

from app import NAME
from app.util import File, JobStatus


class Backend():
    """Backend for folder-backed boxes."""

    def __init__(self, box_path, box_config):
        self.box_path = box_path
        self.box_config = box_config
        self.folder_path = Path(box_config['folder-path'])

    def box_init(self):
        """Optional box initialization at creation time."""
        pass

    def store(self, src_path, name):
        """Store the given file under the name, return a retrieval key."""
        dst_path = self.folder_path.joinpath(name)
        shutil.copy(str(src_path), str(dst_path))
        return name

    def retrieve_init(self, retrieval_key, options):
        """Initiate a retrieval job, return the job key."""
        return retrieval_key

    def retrieve_status(self, *job_keys):
        """Return the overall JobStatus of the given jobs.

           Returns failure if any state is failure,
           success if all states are success,
           running otherwise.
           """
        states = [self._job_status(k) for k in job_keys]
        if any(s == JobStatus.failure for s in states):
            return JobStatus.failure
        elif all(s == JobStatus.success for s in states):
            return JobStatus.success
        else:
            return JobStatus.running

    def retrieve_finish(self, job_key):
        """Finish the job, return the temporary file's Path."""
        file_path = self.folder_path.joinpath(job_key)
        tmp_path = File.mktemp()
        shutil.copy(str(file_path), str(tmp_path))
        return tmp_path

    def _job_status(self, job_key):
        """Return the JobStatus of the given job."""
        file_path = self.folder_path.joinpath(job_key)
        if file_path.exists():
            return JobStatus.success
        else:
            return JobStatus.failure
