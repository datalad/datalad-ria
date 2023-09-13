"""Collection of fixtures for facilitation test implementations
"""

import getpass
import os
from pathlib import Path
import pytest

from datalad_ria.utils import (
    build_ria_url,
)

from datalad_next.tests.utils import (
    SkipTest,
)

from datalad_ria.tests.utils import (
    assert_ssh_access,
)


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
def ria_sshserver(ria_sshserver_setup, monkeypatch):
    ria_baseurl = build_ria_url(
        protocol='ssh',
        host=ria_sshserver_setup['HOST'],
        user=ria_sshserver_setup['SSH_LOGIN'],
        path=ria_sshserver_setup['SSH_PATH'],
    )
    with monkeypatch.context() as m:
        m.setenv("DATALAD_SSH_IDENTITYFILE", ria_sshserver_setup['SSH_SECKEY'])
        yield ria_baseurl, ria_sshserver_setup['LOCALPATH']
