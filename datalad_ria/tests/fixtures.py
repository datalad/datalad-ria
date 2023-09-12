"""Collection of fixtures for facilitation test implementations
"""

import getpass
import os
from pathlib import Path
import pytest
import subprocess

from datalad_next.tests.utils import (
    SkipTest,
)


def verify_ssh_access(host, port, login, seckey, path, localpath):
    # we can only handle openssh
    ssh_bin = os.environ.get('DATALAD_SSH_EXECUTABLE', 'ssh')

    ssh_call = [
        ssh_bin,
        '-i', seckey,
        '-p', port,
        f'{login}@{host}',
    ]
    # now try if this is a viable configuration
    # verify execute and write permissions (implicitly also POSIX path handling
    subprocess.run(
        ssh_call + [f"bash -c 'mkdir -p {path} && touch {path}/datalad-tests-probe'"],
        check=True,
    )
    if localpath:
        # check if a given
        assert (Path(localpath) / 'datalad-tests-probe').exists()
    subprocess.run(
        ssh_call + [f"bash -c 'rm {path}/datalad-tests-probe'"],
        check=True,
    )
    if localpath:
        assert not (Path(localpath) / 'datalad-tests-probe').exists()


@pytest.fixture(autouse=False, scope="session")
def ria_sshserver(tmp_path_factory):
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

    verify_ssh_access(host, port, login, seckey, path, localpath)

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
