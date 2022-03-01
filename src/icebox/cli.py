import logging
import os
from pathlib import Path

import click

from icebox import NAME
from icebox.box import get_box


LOG_FORMAT = '%(asctime)s %(levelname)s %(name)s %(message)s'


@click.group()
@click.option(
    '--config', '-c',
    help='Base configuration directory.',
    type=click.Path(file_okay=False))
@click.pass_context
def icebox(ctx, config):
    """Encrypting Cold Storage Client"""
    if os.environ.get('ICEBOX_DEBUG') == 'true':
        logging.basicConfig(format=LOG_FORMAT)
        logging.getLogger(NAME).setLevel(logging.DEBUG)
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
    box = get_box(base_path, box_name)
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
    try:
        box.init()
    except Exception as e:
        raise click.ClickException('Initialization failed. ({})'.format(e))
    click.echo('Box initialized.')
    click.echo(f'Your encryption keys are in {box.path}')
    click.echo('Make sure to protect and backup this directory!')


@init.command('s3')
@click.argument('bucket')
@click.option(
    '--class', '-c', 'storage_class',
    help='Storage class. (Default: DEEP_ARCHIVE)',
    type=click.Choice(['GLACIER', 'DEEP_ARCHIVE']),
    default='DEEP_ARCHIVE')
@click.option(
    '--tier', '-t',
    help='Retrieval tier. (Default: Bulk)',
    type=click.Choice(['Standard', 'Bulk']),
    default='Bulk')
@click.option('--profile', '-p', help='AWS profile.', default='default')
@click.pass_context
def init_s3(ctx, bucket, storage_class, tier, profile):
    """Create an Amazon S3-backed box."""
    box = ctx.obj['box']
    box.config['backend'] = 's3'
    box.config['bucket'] = bucket
    box.config['storage_class'] = storage_class
    box.config['tier'] = tier
    box.config['profile'] = profile
    try:
        box.init()
    except Exception as e:
        raise click.ClickException('Initialization failed. ({})'.format(e))
    click.echo('Box initialized.')
    click.echo(f'Your encryption keys are in {box.path}')
    click.echo('Make sure to protect and backup this directory!')


@icebox.command()
@click.argument('box-name')
@click.argument('source', type=click.Path(exists=True))
@click.option('--comment', help='Source comment.')
@click.pass_context
def put(ctx, box_name, source, comment):
    """Store data in a box."""
    base_path = ctx.obj['base_path']
    box = get_box(base_path, box_name)
    if not box.exists():
        raise click.ClickException('Box not found.')
    src_path = Path(source)
    src_name = src_path.name
    if box.contains(src_name):
        raise click.ClickException('Source already exists in box.')
    click.echo(f'Storing {src_name} in box.')
    try:
        box.store(src_path, comment)
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
@click.pass_context
def get(ctx, box_name, source, destination, option):
    """Retrieve data from a box."""
    base_path = ctx.obj['base_path']
    box = get_box(base_path, box_name)
    if not box.exists():
        raise click.ClickException('Box not found.')
    if not box.contains(source):
        raise click.ClickException('Source not found in box.')
    click.echo(f'Retrieving {source} from box.')
    dst_path = Path(destination)
    backend_options = dict(o.split('=') for o in option)
    try:
        box.retrieve(source, dst_path, backend_options)
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
    box = get_box(base_path, box_name)
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
    box = get_box(base_path, box_name)
    if not box.exists():
        raise click.ClickException('Box not found.')
    for source in box.sources():
        if source.comment:
            click.echo(f'{source.name} ({source.comment})')
        else:
            click.echo(source.name)


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
    box = get_box(base_path, box_name)
    if not box.exists():
        raise click.ClickException('Box not found.')
    click.echo('Refreshing box.')
    backend_options = dict(o.split('=') for o in option)
    try:
        duplicates, singles = box.refresh(backend_options)
        for d in duplicates:
            msg = ('WARNING: Duplicate found for {}, data key: "{}", ' +
                   'meta key: "{}"')
            click.echo(msg.format(d.name, d.data_key, d.meta_key))
        for name, key in singles.items():
            msg = 'WARNING: Unmatched backend name {}, key: "{}"'
            click.echo(msg.format(name, key))
    except Exception as e:
        raise click.ClickException(f'Operation failed. ({e})')
    click.echo('Refreshed box.')
