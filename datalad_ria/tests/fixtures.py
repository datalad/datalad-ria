"""Collection of fixtures for facilitation test implementations
"""

import getpass
import os
from pathlib import Path
import pytest
import tempfile

from datalad_ria.utils import (
    build_ria_url,
)

from datalad_next.tests.utils import (
    SkipTest,
    rmtree,
)

from datalad_ria.tests.utils import (
    assert_ssh_access,
)

from datalad_next.tests.utils import create_tree


@pytest.fixture(autouse=False, scope="session")
def ria_sshserver_setup(tmp_path_factory):
    if not os.environ.get('DATALAD_TESTS_SSH'):
        raise SkipTest(
            "set DATALAD_TESTS_SSH=1 to enable")

    # query a bunch of recognized configuration environment variables,
    # fill in the blanks, then check if the given configuration is working,
    # and post the full configuration again as ENV vars, to be picked up by
    # the function-scope `datalad_cfg`
    tmp_riaroot = str(tmp_path_factory.mktemp("sshriaroot"))
    host = os.environ.get('DATALAD_TESTS_RIA_SERVER_SSH_HOST', 'localhost')
    port = os.environ.get('DATALAD_TESTS_RIA_SERVER_SSH_PORT', '22')
    login = os.environ.get(
        'DATALAD_TESTS_RIA_SERVER_SSH_LOGIN',
        getpass.getuser())
    seckey = os.environ.get(
        'DATALAD_TESTS_RIA_SERVER_SSH_SECKEY',
        str(Path.home() / '.ssh' / 'id_rsa'))
    path = os.environ.get('DATALAD_TESTS_RIA_SERVER_SSH_PATH', tmp_riaroot)
    # TODO this should not use `tmp_riaroot` unconditionally, but only if
    # the SSH_PATH is known to be the same. This might not be if SSH_PATH
    # is explicitly configured and LOCALPATH is not -- which could be
    # an indication that there is none
    localpath = os.environ.get('DATALAD_TESTS_RIA_SERVER_LOCALPATH', tmp_riaroot)

    assert_ssh_access(host, port, login, seckey, path, localpath)

    info = {}
    # as far as we can tell, this is good, post effective config in ENV
    for v, e in (
            (host, 'HOST'),
            # this is SSH_*, because elsewhere we also have other properties
            # for other services
            (port, 'SSH_PORT'),
            (login, 'SSH_LOGIN'),
            (seckey, 'SSH_SECKEY'),
            (path, 'SSH_PATH'),
            (localpath, 'LOCALPATH'),
    ):
        os.environ[f"DATALAD_TESTS_RIA_SERVER_{e}"] = v
        info[e] = v

    yield info


@pytest.fixture(autouse=False, scope="function")
def ria_sshserver(ria_sshserver_setup, datalad_cfg, monkeypatch):
    ria_baseurl = build_ria_url(
        protocol='ssh',
        host=ria_sshserver_setup['HOST'],
        user=ria_sshserver_setup['SSH_LOGIN'],
        path=ria_sshserver_setup['SSH_PATH'],
    )
    with monkeypatch.context() as m:
        m.setenv("DATALAD_SSH_IDENTITYFILE", ria_sshserver_setup['SSH_SECKEY'])
        # force reload the config manager, to ensure the private key setting
        # makes it into the active config
        datalad_cfg.reload(force=True)
        yield ria_baseurl, ria_sshserver_setup['LOCALPATH']


@pytest.fixture(autouse=False, scope="function")
def populated_dataset(existing_dataset):
    """Creates a new dataset with saved payload"""
    tree = {
        'one.txt': 'content1',
        'three.txt': 'content3',
        'subdir': {
            'two': 'content2',
            'four': 'content4',
        },
    }
    create_tree(existing_dataset.path, tree, archives_leading_dir=False)
    existing_dataset.save(result_renderer='disabled')
    yield existing_dataset


@pytest.fixture(autouse=False, scope="function")
def common_ora_init_opts():
    """Return common initialization arguments for the ora special remote"""
    common_init_opts = ["encryption=none", "type=external", "externaltype=ora",
                        "autoenable=true"]
    yield common_init_opts


@pytest.fixture(autouse=False, scope="function")
def ria_server_localpath() -> Path:
    """Yields a local path that represents the root directory of RIA servers

    Raises ``SkipTest`` if no such local path is not configured. Configure
    via the ``DATALAD_TESTS_RIA_SERVER_LOCALPATH`` environment variable.

    This is a function-scope fixture to enable a per-test skipping, and
    reporting of that event.
    """
    var = 'DATALAD_TESTS_RIA_SERVER_LOCALPATH'
    if var not in os.environ:
        raise SkipTest(f"set {var} to enable")

    localpath = os.environ['DATALAD_TESTS_RIA_SERVER_LOCALPATH']
    localpath = Path(localpath)

    if not localpath.is_dir():
        raise ValueError(f'{var} does not point to an existing directory')

    yield localpath.absolute()


@pytest.fixture(autouse=False, scope="function")
def ria_store_localaccess(ria_server_localpath):
    """Yields an empty RIA store, created and accessible locally

    This fixtures relies on ``ria_server_localpath`` to generate a RIA store
    locally on the file system that can be exposed via a particular server.

    The fixture yield a tuple with ``name`` and ``path`` of the store.
    ``name`` corresponds to a directory underneath the RIA root path that
    represents this store's root path. ``path`` is the local directory
    path (absolute) that match the same store root path.
    """
    with tempfile.TemporaryDirectory(
        # we must set the prefix (at least on older Pythons) to ensure that
        # we get a direct subdirectory underneath `ria_server_localpath`
        # as the tempdir
        prefix='ria',
        dir=ria_server_localpath,
    ) as basepath:
        basepath = Path(basepath)
        error_logs = basepath / 'error_logs'
        version_file = basepath / 'ria-layout-version'
        error_logs.touch()
        version_file.write_text('1')
        yield basepath.name, basepath
        # we do the cleanup ourselves, because we anticipate permission issues
        # with plain deletions, and `ignore_cleanup_errors=True` is only
        # supported from PY3.10 onwards.
        rmtree(basepath)
