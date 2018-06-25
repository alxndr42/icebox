from click.testing import CliRunner
import pytest

from app.cli import icebox


BOX_NAME = 'test'
KEY_ID = 'B1448632CDD25B97D823F1556CF1AB048EFFA419'


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

    def test_invalid_key(self, datadir):
        """Try to create a box with an invalid key."""
        cli = CliWrapper(datadir)
        with pytest.raises(AssertionError) as e:
            cli.init(key='nosuchkey')

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


class CliWrapper():
    """Execute the CLI with test directories."""

    def __init__(self, test_path):
        self._base = test_path.joinpath('base')
        self._backend = test_path.joinpath('backend')
        self._input = test_path.joinpath('input')
        self._output = test_path.joinpath('output')
        self._runner = CliRunner()

    def init(self, box=BOX_NAME, key=KEY_ID, folder=None):
        """Run the init command."""
        base = str(self._base)
        if folder is None:
            folder = str(self._backend)
        result = self._runner.invoke(
            icebox,
            ['-b', base, 'init', box, key, 'folder', folder])
        print(result.output)
        assert result.exit_code == 0

    def put(self, source, box=BOX_NAME):
        """Run the put command."""
        base = str(self._base)
        path = str(self._input.joinpath(source))
        result = self._runner.invoke(
            icebox,
            ['-b', base, 'put', box, path])
        print(result.output)
        assert result.exit_code == 0

    def get(self, source, box=BOX_NAME):
        """Run the get command, return the created Path."""
        base = str(self._base)
        output = str(self._output)
        result = self._runner.invoke(
            icebox,
            ['-b', base, 'get', box, source, output])
        print(result.output)
        assert result.exit_code == 0
        return self._output.joinpath(source)

    def delete(self, source, box=BOX_NAME):
        """Run the delete command."""
        base = str(self._base)
        result = self._runner.invoke(
            icebox,
            ['-b', base, 'delete', box, source])
        print(result.output)
        assert result.exit_code == 0

    def list(self, box=BOX_NAME):
        """Run the list command, return the output."""
        base = str(self._base)
        result = self._runner.invoke(
            icebox,
            ['-b', base, 'list', box])
        print(result.output)
        assert result.exit_code == 0
        return result.output
