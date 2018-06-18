import app.backend.folder
import app.backend.glacier


def get_backend(name, box_path, box_config):
    """Return a backend instance for the given name."""
    if name == 'folder':
        return app.backend.folder.Backend(box_path, box_config)
    elif name == 'glacier':
        return app.backend.glacier.Backend(box_path, box_config)
    else:
        raise Exception('Unsupported backend: ' + name)
