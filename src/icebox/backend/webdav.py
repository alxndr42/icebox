from webdav4.client import Client

from icepack.helper import File

from icebox.data import BackendFile, JobStatus


class Backend():
    """Backend for WebDAV-backed boxes."""

    def __init__(self, box_path, box_config):
        self.box_path = box_path
        self.box_config = box_config
        url = box_config['url']
        username = box_config.get('username')
        password = box_config.get('password')
        if not username:
            raise Exception('No username specified.')
        if not password:
            raise Exception('No password specified.')
        self.client = Client(url, auth=(username, password))

    def box_init(self):
        """Optional box initialization at creation time."""
        self.client.exists('access-test')  # Quick access test

    def store_data(self, src_path, name):
        """Store the data file as name, return a retrieval key."""
        self.client.upload_file(src_path, name)
        return name

    def store_meta(self, src_path, name):
        """Store the metadata file as name, return a retrieval key."""
        self.client.upload_file(src_path, name)
        return name

    def retrieve_init(self, retrieval_key, options):
        """Initiate a retrieval job, return the job key."""
        return retrieval_key

    def retrieve_status(self, job_key):
        """Return the JobStatus of the given job."""
        exists = self.client.exists(job_key)
        if exists:
            return JobStatus.success
        else:
            return JobStatus.failure

    def retrieve_finish(self, job_key):
        """Finish the job, return the temporary file's Path."""
        tmp_path = File.mktemp()
        self.client.download_file(job_key, tmp_path)
        return tmp_path

    def delete(self, retrieval_key):
        """Delete the data for the given retrieval key."""
        self.client.remove(retrieval_key)

    def inventory_init(self):
        """Initiate an inventory job, return the job key."""
        return 'inventory'

    def inventory_status(self, job_key):
        """Return the JobStatus of the given job."""
        return JobStatus.success

    def inventory_finish(self, job_key):
        """Return a list of BackendFiles."""
        result = []
        for c in self.client.ls('', detail=True):
            if c['type'] != 'file':
                continue
            file = BackendFile(c['name'], c['name'], c['content_length'])
            result.append(file)
        return result
