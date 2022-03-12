import boto3

from icepack.helper import File

from icebox.data import BackendFile, JobStatus


RESTORE_COMPLETE = 'ongoing-request="false"'
RESTORE_RUNNING = 'ongoing-request="true"'


class Backend():
    """Backend for Amazon S3-backed boxes."""

    def __init__(self, box_path, box_config):
        self.box_path = box_path
        self.box_config = box_config
        self.storage_class = box_config['storage_class']
        self.tier = box_config['tier']
        profile = box_config['profile']
        bucket = box_config['bucket']
        session = boto3.session.Session(profile_name=profile)
        self.bucket = session.resource('s3').Bucket(bucket)

    def box_init(self):
        """Optional box initialization at creation time."""
        list(self.bucket.objects.limit(count=1))  # Quick access test

    def store_data(self, src_path, name):
        """Store the data file as name, return a retrieval key."""
        return self._store(str(src_path), name, self.storage_class)

    def store_meta(self, src_path, name):
        """Store the metadata file as name, return a retrieval key."""
        return self._store(str(src_path), name, 'STANDARD')

    def retrieve_init(self, retrieval_key, options):
        """Initiate a retrieval job, return the job key."""
        obj = self.bucket.Object(retrieval_key)
        if obj.storage_class and obj.restore is None:
            tier = options.get('Tier', self.tier)
            request = {
                'Days': 1,
                'GlacierJobParameters': {'Tier': tier},
            }
            obj.restore_object(RestoreRequest=request)
        return retrieval_key

    def retrieve_status(self, job_key):
        """Return the JobStatus of the given job."""
        obj = self.bucket.Object(job_key)
        if obj.storage_class is None:
            return JobStatus.success
        if obj.restore is None:
            return JobStatus.failure
        if RESTORE_RUNNING in obj.restore:
            return JobStatus.running
        if RESTORE_COMPLETE in obj.restore:
            return JobStatus.success
        raise Exception(f'Unsupported restore state: {obj.restore}')

    def retrieve_finish(self, job_key):
        """Finish the job, return the temporary file's Path."""
        obj = self.bucket.Object(job_key)
        if obj.storage_class and \
                (obj.restore is None or RESTORE_COMPLETE not in obj.restore):
            raise Exception('Job was not successful.')
        tmp_path = File.mktemp()
        obj.download_file(str(tmp_path))
        return tmp_path

    def delete(self, retrieval_key):
        """Delete the data for the given retrieval key."""
        obj = self.bucket.Object(retrieval_key)
        obj.delete()

    def inventory_init(self):
        """Initiate an inventory job, return the job key."""
        return 'inventory'

    def inventory_status(self, job_key):
        """Return the JobStatus of the given job."""
        return JobStatus.success

    def inventory_finish(self, job_key):
        """Return a list of BackendFiles."""
        objects = self.bucket.objects.all()
        return [BackendFile(o.key, o.key, o.size) for o in objects]

    def _store(self, filename, name, storage_class):
        """Store filename as name, return a retrieval key."""
        args = {'StorageClass': storage_class}
        self.bucket.upload_file(filename, name, ExtraArgs=args)
        return name
