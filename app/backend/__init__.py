import app.backend.folder


def get_backend(name, box_path, box_config):
    """Return a backend instance for the given name."""
    if name == 'folder':
        return app.backend.folder.Backend(box_path, box_config)
    else:
        raise Exception('Unsupported backend: ' + name)
