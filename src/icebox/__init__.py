NAME = 'icebox'
VERSION = '1.0.0'

SECRET_KEY = 'identity'  # nosec No secret
PUBLIC_KEY = 'identity.pub'
ALLOWED_SIGNERS = 'allowed_signers'


def human_readable_size(size, decimals=2):
    """Return size as a human-readable string, i.e. '42.23 MB'."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(size) < 1024.0 or unit == 'TB':
            break
        size /= 1024.0
    return f'{size:.{decimals}f} {unit}'
