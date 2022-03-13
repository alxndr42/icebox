import icebox.backend.folder
import icebox.backend.s3
import icebox.backend.webdav


def get_backend(name, box_path, box_config):
    """Return a backend instance for the given name."""
    if name == 'folder':
        return icebox.backend.folder.Backend(box_path, box_config)
    elif name == 's3':
        return icebox.backend.s3.Backend(box_path, box_config)
    elif name == 'webdav':
        return icebox.backend.webdav.Backend(box_path, box_config)
    else:
        raise Exception('Unsupported backend: ' + name)
