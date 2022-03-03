import shutil

from click.testing import CliRunner
from icepack.model import Compression
import pytest

from icebox import NAME, VERSION
from icebox.box import DATA_SUFFIX, META_SUFFIX
from icebox.cli import icebox


BOX_NAME = 'test'


class TestFolderBackend():
    """Test the CLI using a folder-backed box."""

    def test_file(self, datadir):
        """Store and retrieve a file."""
        cli = CliWrapper(datadir)
        cli.init()
        cli.put('test')
        file = cli.get('test')
        assert file.is_file()
        content = file.read_text(encoding='utf-8')
        assert content == 'test\n'

    def test_file_bz2_compression(self, datadir):
        """Store and retrieve a file with "bz2" compression."""
        cli = CliWrapper(datadir)
        cli.init()
        cli.put('test', compression=Compression.BZ2)
        file = cli.get('test')
        assert file.is_file()
        content = file.read_text(encoding='utf-8')
        assert content == 'test\n'

    def test_file_none_compression(self, datadir):
        """Store and retrieve a file with "none" compression."""
        cli = CliWrapper(datadir)
        cli.init()
        cli.put('test', compression=Compression.NONE)
        file = cli.get('test')
        assert file.is_file()
        content = file.read_text(encoding='utf-8')
        assert content == 'test\n'

    def test_umlaut_file(self, datadir):
        """Store and retrieve a file with an umlaut in name and content."""
        cli = CliWrapper(datadir)
        cli.init()
        cli.put('täst')
        file = cli.get('täst')
        assert file.is_file()
        content = file.read_text(encoding='utf-8')
        assert content == 'täst\n'

    def test_folder(self, datadir):
        """Store and retrieve a folder."""
        cli = CliWrapper(datadir)
        cli.init()
        cli.put('folder')
        folder = cli.get('folder')
        assert folder.is_dir()
        foo = folder.joinpath('foo')
        assert foo.is_file()
        content = foo.read_text(encoding='utf-8')
        assert content == 'foo\n'
        bar = folder.joinpath('bar')
        assert bar.is_file()
        content = bar.read_text(encoding='utf-8')
        assert content == 'bar\n'

    def test_duplicate_box(self, datadir):
        """Try to create a box twice."""
        cli = CliWrapper(datadir)
        cli.init()
        with pytest.raises(AssertionError) as e:
            cli.init()

    def test_invalid_folder(self, datadir):
        """Try to create a box with an invalid folder."""
        cli = CliWrapper(datadir)
        with pytest.raises(AssertionError) as e:
            cli.init(folder='/nosuchfolder')

    def test_duplicate_source(self, datadir):
        """Try to store a source twice."""
        cli = CliWrapper(datadir)
        cli.init()
        cli.put('test')
        with pytest.raises(AssertionError) as e:
            cli.put('test')

    def test_invalid_box(self, datadir):
        """Try to store and retrieve using an invalid box."""
        cli = CliWrapper(datadir)
        with pytest.raises(AssertionError) as e:
            cli.put('test')
        with pytest.raises(AssertionError) as e:
            cli.get('test')

    def test_delete(self, datadir):
        """Delete a file."""
        cli = CliWrapper(datadir)
        cli.init()
        cli.put('test')
        cli.delete('test')
        with pytest.raises(AssertionError) as e:
            cli.get('test')
        with pytest.raises(AssertionError) as e:
            cli.delete('test')

    def test_list(self, datadir):
        """Test the list command."""
        cli = CliWrapper(datadir)
        cli.init()
        output = cli.list()
        assert output == ''
        cli.put('test')
        output = cli.list()
        assert output == 'test\n'
        cli.put('folder')
        output = cli.list()
        assert output == 'folder\ntest\n'
        cli.delete('folder')
        output = cli.list()
        assert output == 'test\n'

    def test_refresh(self, datadir):
        """Test the refresh command."""
        cli = CliWrapper(datadir)
        cli.init()
        cli.put('test')
        for src in cli.backend.iterdir():
            if src.name.endswith(DATA_SUFFIX):
                dst = src.parent.joinpath('clone' + DATA_SUFFIX)
                shutil.copy(str(src), str(dst))
            elif src.name.endswith(META_SUFFIX):
                dst = src.parent.joinpath('clone' + META_SUFFIX)
                shutil.copy(str(src), str(dst))
        cli.delete('test')
        output = cli.list()
        assert output == ''
        cli.refresh()
        output = cli.list()
        assert output == 'test\n'

    def test_version(key_path, datadir):
        """Test the version command."""
        cli = CliWrapper(datadir)
        output = cli.version()
        assert output == f'{NAME} {VERSION}\n'


class CliWrapper():
    """Execute the CLI with test directories."""

    def __init__(self, test_path):
        self._base = test_path.joinpath('base')
        self._backend = test_path.joinpath('backend')
        self._input = test_path.joinpath('input')
        self._output = test_path.joinpath('output')
        self._runner = CliRunner()

    @property
    def backend(self):
        return self._backend

    def init(self, box=BOX_NAME, folder=None):
        """Run the init command."""
        base = str(self._base)
        if folder is None:
            folder = str(self._backend)
        result = self._runner.invoke(
            icebox,
            ['-c', base, 'init', box, 'folder', folder])
        print(result.output)
        assert result.exit_code == 0

    def put(self, source, box=BOX_NAME, compression=Compression.GZ):
        """Run the put command."""
        base = str(self._base)
        path = str(self._input.joinpath(source))
        result = self._runner.invoke(
            icebox,
            ['-c', base, 'put', box, path])
        print(result.output)
        assert result.exit_code == 0

    def get(self, source, box=BOX_NAME):
        """Run the get command, return the created Path."""
        base = str(self._base)
        output = str(self._output)
        result = self._runner.invoke(
            icebox,
            ['-c', base, 'get', box, source, '-d', output])
        print(result.output)
        assert result.exit_code == 0
        return self._output.joinpath(source)

    def delete(self, source, box=BOX_NAME):
        """Run the delete command."""
        base = str(self._base)
        result = self._runner.invoke(
            icebox,
            ['-c', base, 'delete', box, source])
        print(result.output)
        assert result.exit_code == 0

    def list(self, box=BOX_NAME):
        """Run the list command, return the output."""
        base = str(self._base)
        result = self._runner.invoke(
            icebox,
            ['-c', base, 'list', box])
        print(result.output)
        assert result.exit_code == 0
        return result.output

    def refresh(self, box=BOX_NAME):
        """Run the refresh command."""
        base = str(self._base)
        result = self._runner.invoke(
            icebox,
            ['-c', base, 'refresh', box])
        print(result.output)
        assert result.exit_code == 0

    def version(self):
        """Run the version command."""
        base = str(self._base)
        result = self._runner.invoke(
            icebox,
            ['-c', base, 'version'])
        print(result.output)
        assert result.exit_code == 0
        return result.output
