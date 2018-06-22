import io
from pathlib import Path
import shutil

import boto3
from botocore.utils import calculate_tree_hash

from app import NAME
from app.util import File, JobStatus, OffsetRangeWrapper


PART_SIZE = 64 * 1024 * 1024
MULTIPART_LIMIT = 100 * 1024 * 1024


class Backend():
    """Backend for Amazon Glacier-backed boxes."""

    def __init__(self, box_path, box_config):
        self.box_path = box_path
        self.box_config = box_config
        self.tier = box_config['tier']
        profile = box_config['profile']
        vault = box_config['vault']
        self.session = boto3.session.Session(profile_name=profile)
        self.glacier = self.session.resource('glacier')
        self.vault = self.glacier.Vault('-', vault)

    def box_init(self):
        """Optional box initialization at creation time."""
        self.vault.last_inventory_date  # Quick access test

    def store(self, src_path, name):
        """Store the given file under the name, return a retrieval key."""
        size = src_path.stat().st_size
        if size < MULTIPART_LIMIT:
            with open(src_path, 'rb') as f:
                result = self.vault.upload_archive(
                    archiveDescription=name, body=f)
            archive_id = result.id
        else:
            mpu = self.vault.initiate_multipart_upload(
                archiveDescription=name, partSize=str(PART_SIZE))
            offset = 0
            with open(src_path, 'rb') as f:
                treehash = calculate_tree_hash(f)
                while offset < size:
                    max_offset = min(size, offset + PART_SIZE)
                    r = 'bytes {}-{}/*'.format(offset, max_offset - 1)
                    w = OffsetRangeWrapper(f, offset, max_offset)
                    mpu.upload_part(range=r, body=w)
                    offset += PART_SIZE
            result = mpu.complete(archiveSize=str(size), checksum=treehash)
            archive_id = result['archiveId']
        return archive_id

    def retrieve_init(self, retrieval_key, options):
        """Initiate a retrieval job, return the job key."""
        archive = self.vault.Archive(retrieval_key)
        params = {
            'Type': 'archive-retrieval',
            'ArchiveId': retrieval_key,
            'Tier': self.tier,
        }
        if 'Tier' in options:
            params['Tier'] = options['Tier']
        job = archive.initiate_archive_retrieval(jobParameters=params)
        return job.id

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
        job = self.vault.Job(job_key)
        job.load()
        if job.status_code != 'Succeeded':
            raise Exception('Job was not successful.')

        response = job.get_output()
        src = response['body']
        tmp_path = File.mktemp()
        with open(tmp_path, 'wb') as dst:
            shutil.copyfileobj(src, dst, 65536)
        return tmp_path

    def _job_status(self, job_key):
        """Return the JobStatus of the given job."""
        job = self.vault.Job(job_key)
        job.load()
        if job.status_code == 'Succeeded':
            return JobStatus.success
        elif job.status_code == 'Failed':
            return JobStatus.failure
        else:
            return JobStatus.running
