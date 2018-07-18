import logging
import os
from pathlib import Path

import click

from app import NAME
from app.box import get_box
from app.gpg import GPG


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
LOG_FORMAT = '%(asctime)s %(levelname)s %(name)s %(message)s'


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option(
    '--base-dir', '-b',
    help='Base directory for configuration data.',
    type=click.Path(file_okay=False))
@click.pass_context
def icebox(ctx, base_dir):
    """Encrypting command-line client for Amazon Glacier."""
    if os.environ.get('ICEBOX_DEBUG') == 'true':
        logging.basicConfig(format=LOG_FORMAT)
        logging.getLogger('app').setLevel(logging.DEBUG)
    ctx.obj = {}
    if base_dir is None:
        ctx.obj['base'] = Path(click.get_app_dir(NAME))
    else:
        ctx.obj['base'] = Path(base_dir)


@icebox.group()
@click.argument('box-name')
@click.argument('key-id')
@click.pass_context
def init(ctx, box_name, key_id):
    """Create a new box."""
    base_path = ctx.obj['base']
    box = get_box(base_path, box_name)
    if box.exists():
        click.echo('Box already exists.')
        ctx.exit(1)

    gpg = GPG(base_path.joinpath('GPG'))
    if not gpg.valid_key_id(key_id):
        click.echo('Invalid key ID.')
        ctx.exit(1)

    box.gpg = gpg
    box.key = key_id
    ctx.obj['box'] = box


@init.command('folder')
@click.argument(
    'folder-path',
    type=click.Path(
        exists=True, file_okay=False, resolve_path=True, writable=True))
@click.pass_context
def init_folder(ctx, folder_path):
    """Create a folder-backed box."""
    box = ctx.obj['box']
    box.config['backend'] = 'folder'
    box.config['folder-path'] = folder_path
    try:
        box.init()
    except Exception as e:
        msg = 'Box initialization failed. ({})'.format(e)
        click.echo(msg)
        ctx.exit(1)
    click.echo('Box initialized.')


@init.command('glacier')
@click.argument('vault')
@click.option('--profile', '-p', help='AWS profile', default='default')
@click.option(
    '--tier', '-t',
    help='Default retrieval tier',
    type=click.Choice(['Expedited', 'Standard', 'Bulk']),
    default='Standard')
@click.pass_context
def init_glacier(ctx, vault, profile, tier):
    """Create an Amazon Glacier-backed box."""
    box = ctx.obj['box']
    box.config['backend'] = 'glacier'
    box.config['vault'] = vault
    box.config['profile'] = profile
    box.config['tier'] = tier
    try:
        box.init()
    except Exception as e:
        msg = 'Box initialization failed. ({})'.format(e)
        click.echo(msg)
        ctx.exit(1)
    click.echo('Box initialized.')


@icebox.command()
@click.argument('box-name')
@click.argument('source', type=click.Path(exists=True))
@click.pass_context
def put(ctx, box_name, source):
    """Store data in a box."""
    base_path = ctx.obj['base']
    box = get_box(base_path, box_name)
    if not box.exists():
        click.echo('Box not found.')
        ctx.exit(1)

    src_path = Path(source)
    src_name = src_path.name
    if box.contains(src_name):
        click.echo('Source already exists in box.')
        ctx.exit(1)

    click.echo('Storing {} in box.'.format(src_name))
    try:
        box.gpg = GPG(base_path.joinpath('GPG'))
        box.store(src_path)
    except Exception as e:
        click.echo(str(e))
        ctx.exit(1)
    click.echo('Stored {} in box.'.format(src_name))


@icebox.command()
@click.argument('box-name')
@click.argument('source')
@click.option(
    '--destination', '-d',
    help='Destination (default: current directory)',
    type=click.Path(
        exists=True, file_okay=False, resolve_path=True, writable=True),
    default='.')
@click.option(
    '--option', '-o',
    help='A key=value option for the backend operation.',
    multiple=True)
@click.pass_context
def get(ctx, box_name, source, destination, option):
    """Retrieve data from a box."""
    base_path = ctx.obj['base']
    box = get_box(base_path, box_name)
    if not box.exists():
        click.echo('Box not found.')
        ctx.exit(1)

    if not box.contains(source):
        click.echo('Source not found in box.')
        ctx.exit(1)

    click.echo('Retrieving {} from box.'.format(source))
    dst_path = Path(destination)
    backend_options = dict(o.split('=') for o in option)
    try:
        box.gpg = GPG(base_path.joinpath('GPG'))
        box.retrieve(source, dst_path, backend_options)
    except Exception as e:
        click.echo(str(e))
        ctx.exit(1)
    click.echo('Retrieved {} from box.'.format(source))


@icebox.command()
@click.argument('box-name')
@click.argument('source')
@click.pass_context
def delete(ctx, box_name, source):
    """Delete data from a box."""
    base_path = ctx.obj['base']
    box = get_box(base_path, box_name)
    if not box.exists():
        click.echo('Box not found.')
        ctx.exit(1)

    if not box.contains(source):
        click.echo('Source not found in box.')
        ctx.exit(1)

    try:
        box.delete(source)
    except Exception as e:
        click.echo(str(e))
        ctx.exit(1)
    click.echo('Deleted {} from box.'.format(source))


@icebox.command()
@click.argument('box-name')
@click.pass_context
def list(ctx, box_name):
    """List the data in a box."""
    base_path = ctx.obj['base']
    box = get_box(base_path, box_name)
    if not box.exists():
        click.echo('Box not found.')
        ctx.exit(1)

    for source in box.sources():
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
    base_path = ctx.obj['base']
    box = get_box(base_path, box_name)
    if not box.exists():
        click.echo('Box not found.')
        ctx.exit(1)

    click.echo('Refreshing box.')
    backend_options = dict(o.split('=') for o in option)
    try:
        box.gpg = GPG(base_path.joinpath('GPG'))
        duplicates, singles = box.refresh(backend_options)
        for d in duplicates:
            msg = ('WARNING: Duplicate found for {}, data key: "{}", ' +
                   'meta key: "{}"')
            click.echo(msg.format(d.name, d.data_key, d.meta_key))
        for name, key in singles.items():
            msg = 'WARNING: Unmatched backend name {}, key: "{}"'
            click.echo(msg.format(name, key))
    except Exception as e:
        click.echo(str(e))
        ctx.exit(1)
    click.echo('Refreshed box.')
