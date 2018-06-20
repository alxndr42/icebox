from pathlib import Path

import click

from app import NAME
from app.box import get_box
from app.gpg import GPG


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option(
    '--base-dir', '-b',
    help='Base directory for configuration data.',
    type=click.Path(file_okay=False))
@click.pass_context
def icebox(ctx, base_dir):
    """Store GnuPG-encrypted files in Amazon Glacier."""
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
        click.echo('Source name already exists in box.')
        ctx.exit(1)

    gpg = GPG(base_path.joinpath('GPG'))
    data_path = None
    meta_path = None
    try:
        data_path, meta_path = gpg.encrypt(src_path, box.key)
        box.store(data_path, meta_path, src_name)
    except Exception as e:
        click.echo(str(e))
        ctx.exit(1)
    finally:
        if data_path and data_path.exists():
            data_path.unlink()
        if meta_path and meta_path.exists():
            meta_path.unlink()
    click.echo('Source stored in box.')


@icebox.command()
@click.argument('box-name')
@click.argument('source')
@click.argument(
    'destination',
    type=click.Path(
        exists=True, file_okay=False, resolve_path=True, writable=True))
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
        click.echo('Source name not found in box.')
        ctx.exit(1)

    backend_options = dict(o.split('=') for o in option)
    gpg = GPG(base_path.joinpath('GPG'))
    data_path = None
    meta_path = None
    try:
        data_path, meta_path = box.retrieve(source, backend_options)
        gpg.decrypt(data_path, meta_path, Path(destination))
    except Exception as e:
        click.echo(str(e))
        ctx.exit(1)
    finally:
        if data_path and data_path.exists():
            data_path.unlink()
        if meta_path and meta_path.exists():
            meta_path.unlink()
    click.echo('Source retrieved from box.')
