from pathlib import Path

import click
from icepack import Age, SSH
from icepack.model import Compression

from icebox import NAME, VERSION, human_readable_size
from icebox.box import Box


@click.group()
@click.option(
    '--config', '-c',
    help='Base configuration directory.',
    type=click.Path(file_okay=False))
@click.pass_context
def icebox(ctx, config):
    """Encrypting Cold Storage Client"""
    ctx.obj = {}
    if config is None:
        base_path = Path(click.get_app_dir(NAME))
    else:
        base_path = Path(config)
    base_path.mkdir(mode=0o700, parents=True, exist_ok=True)
    ctx.obj['base_path'] = base_path


@icebox.group()
@click.argument('box-name')
@click.pass_context
def init(ctx, box_name):
    """Create a new box."""
    base_path = ctx.obj['base_path']
    box = Box(base_path.joinpath(box_name))
    if box.exists():
        raise click.ClickException('Box already exists.')
    ctx.obj['box'] = box


@init.command('folder')
@click.argument(
    'folder-path',
    type=click.Path(exists=True, file_okay=False, writable=True))
@click.pass_context
def init_folder(ctx, folder_path):
    """Create a folder-backed box."""
    box = ctx.obj['box']
    box.config['backend'] = 'folder'
    box.config['folder_path'] = folder_path
    click.echo('Initializing box.')
    try:
        box.init(log=click.echo)
    except Exception as e:
        raise click.ClickException('Initialization failed. ({})'.format(e))
    click.echo(f'- Your encryption keys are in {box.path}')
    click.echo('- Make sure to protect and backup this directory!')
    click.echo('Box initialized.')


@init.command('s3')
@click.argument('bucket')
@click.option(
    '--class', '-c', 'storage_class',
    help='Storage class. (Default: DEEP_ARCHIVE)',
    type=click.Choice(['GLACIER', 'DEEP_ARCHIVE']),
    default='DEEP_ARCHIVE')
@click.option('--profile', '-p', help='AWS profile.', default='default')
@click.pass_context
def init_s3(ctx, bucket, storage_class, profile):
    """Create an Amazon S3-backed box."""
    box = ctx.obj['box']
    box.config['backend'] = 's3'
    box.config['bucket'] = bucket
    box.config['profile'] = profile
    box.config['storage_class'] = storage_class
    box.config['tier'] = 'Bulk'
    click.echo('Initializing box.')
    try:
        box.init(log=click.echo)
    except Exception as e:
        raise click.ClickException('Initialization failed. ({})'.format(e))
    click.echo(f'- Your encryption keys are in {box.path}')
    click.echo('- Make sure to protect and backup this directory!')
    click.echo('Box initialized.')


@init.command('webdav')
@click.argument('url')
@click.option(
    '--username', '-u',
    help='WebDAV username.',
    prompt='WebDAV username')
@click.password_option(
    '--password', '-p',
    help='WebDAV password.',
    prompt='WebDAV password')
@click.pass_context
def init_webdav(ctx, url, username, password):
    """Create a WebDAV-backed box."""
    box = ctx.obj['box']
    box.config['backend'] = 'webdav'
    box.config['url'] = url
    if username:
        box.config['username'] = username
    if password:
        box.config['password'] = password
    click.echo('Initializing box.')
    try:
        box.init(log=click.echo)
    except Exception as e:
        raise click.ClickException('Initialization failed. ({})'.format(e))
    click.echo(f'- Your encryption keys are in {box.path}')
    click.echo('- Make sure to protect and backup this directory!')
    click.echo('Box initialized.')


@icebox.command()
@click.argument('box-name')
@click.argument('source', type=click.Path(exists=True))
@click.option('--comment', help='Source comment.')
@click.option(
    '--compression', '-c',
    help=f'Compression for all files. (Default: {Compression.GZ})',
    type=click.Choice([c.value for c in Compression]),
    default=Compression.GZ)
@click.option(
    '--mode',
    help='Store file/directory modes.',
    is_flag=True)
@click.option(
    '--mtime',
    help='Store file/directory modification times.',
    is_flag=True)
@click.pass_context
def put(ctx, box_name, source, comment, compression, mode, mtime):
    """Store data in a box."""
    base_path = ctx.obj['base_path']
    box = Box(base_path.joinpath(box_name))
    if not box.exists():
        raise click.ClickException('Box not found.')
    src_path = Path(source)
    src_name = src_path.name
    if box.contains(src_name):
        raise click.ClickException('Source already exists in box.')
    click.echo(f'Storing {src_name} in box.')
    try:
        box.store(
            src_path,
            comment=comment,
            compression=compression,
            mode=mode,
            mtime=mtime,
            log=click.echo)
    except Exception as e:
        raise click.ClickException(f'Operation failed. ({e})')
    click.echo(f'Stored {src_name} in box.')


@icebox.command()
@click.argument('box-name')
@click.argument('source')
@click.option(
    '--destination', '-d',
    help='Destination directory. (Default: Current directory)',
    type=click.Path(exists=True, file_okay=False, writable=True),
    default='.')
@click.option(
    '--option', '-o',
    help='A key=value option for the backend operation.',
    multiple=True)
@click.option(
    '--mode',
    help='Restore file/directory modes.',
    is_flag=True)
@click.option(
    '--mtime',
    help='Restore file/directory modification times.',
    is_flag=True)
@click.pass_context
def get(ctx, box_name, source, destination, option, mode, mtime):
    """Retrieve data from a box."""
    base_path = ctx.obj['base_path']
    box = Box(base_path.joinpath(box_name))
    if not box.exists():
        raise click.ClickException('Box not found.')
    if not box.contains(source):
        raise click.ClickException('Source not found in box.')
    click.echo(f'Retrieving {source} from box.')
    dst_path = Path(destination)
    backend_options = dict(o.split('=') for o in option)
    try:
        box.retrieve(
            source,
            dst_path,
            backend_options,
            mode=mode,
            mtime=mtime,
            log=click.echo)
    except Exception as e:
        raise click.ClickException(f'Operation failed. ({e})')
    click.echo(f'Retrieved {source} from box.')


@icebox.command()
@click.argument('box-name')
@click.argument('source')
@click.pass_context
def delete(ctx, box_name, source):
    """Delete data from a box."""
    base_path = ctx.obj['base_path']
    box = Box(base_path.joinpath(box_name))
    if not box.exists():
        raise click.ClickException('Box not found.')
    if not box.contains(source):
        raise click.ClickException('Source not found in box.')
    try:
        box.delete(source)
    except Exception as e:
        raise click.ClickException(f'Operation failed. ({e})')
    click.echo(f'Deleted {source} from box.')


@icebox.command()
@click.argument('box-name')
@click.pass_context
def list(ctx, box_name):
    """List the data in a box."""
    base_path = ctx.obj['base_path']
    box = Box(base_path.joinpath(box_name))
    if not box.exists():
        raise click.ClickException('Box not found.')
    total_size = 0
    for source in box.sources():
        total_size += source.size
        size = human_readable_size(source.size)
        if source.comment:
            click.echo(f'{source.name} | {size} | {source.comment}')
        else:
            click.echo(f'{source.name} | {size}')
    click.echo(f'Total size: {human_readable_size(total_size)}')


@icebox.command()
@click.argument('box-name')
@click.option(
    '--option', '-o',
    help='A key=value option for the backend operation.',
    multiple=True)
@click.pass_context
def refresh(ctx, box_name, option):
    """Refresh local information for a box."""
    base_path = ctx.obj['base_path']
    box = Box(base_path.joinpath(box_name))
    if not box.exists():
        raise click.ClickException('Box not found.')
    click.echo('Refreshing box.')
    backend_options = dict(o.split('=') for o in option)
    try:
        box.refresh(backend_options, log=click.echo)
    except Exception as e:
        raise click.ClickException(f'Operation failed. ({e})')
    click.echo('Refreshed box.')


@icebox.command()
@click.option(
    '--dependencies', '-d',
    help='Check the dependencies.',
    is_flag=True)
@click.pass_context
def version(ctx, dependencies):
    """Show the version information."""
    click.echo(f'{NAME} {VERSION}')
    if not dependencies:
        return
    age_version, age_keygen = Age.version()
    if age_version:
        click.echo(f'✅ age found. (Version: {age_version})')
    else:
        click.echo(f'❌ age not found.')
    if age_keygen:
        click.echo(f'✅ age-keygen found.')
    else:
        click.echo(f'❌ age-keygen not found.')
    ssh_version, ssh_keygen = SSH.version()
    if ssh_version:
        click.echo(f'✅ ssh found. (Version: {ssh_version})')
    else:
        click.echo(f'❌ ssh not found.')
    if ssh_keygen:
        click.echo(f'✅ ssh-keygen found.')
    else:
        click.echo(f'❌ ssh-keygen not found.')
