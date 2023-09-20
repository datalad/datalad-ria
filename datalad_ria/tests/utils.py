from __future__ import annotations


import os
from pathlib import Path
import subprocess


def assert_ssh_access(
    host: str,
    port: str,
    login: str,
    seckey: str,
    path: str,
    localpath: str | None = None,
):
    """Test for a working SSH connection and sufficient permissions to write

    This helper establishes a connection to an SSH server identified by
    ``host`` and ``port``, using a given SSH private key file (``seckey``) for
    authentication.  Once logged in successfully, it tries to create a
    directory and a file at POSIX ``path`` on the server. If ``localpath`` is
    given, it must be a representation of that server-side path on the local
    file system (e.g., a bindmount), and the helper tests whether the created
    content is also reflected in this directory.
    """
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
        ssh_call + [
            f"bash -c 'mkdir -p {path} && touch {path}/datalad-tests-probe'"],
        stdin=subprocess.PIPE,
        check=True,
    )
    if localpath:
        # check if a given
        assert (Path(localpath) / 'datalad-tests-probe').exists()
    subprocess.run(
        ssh_call + [f"bash -c 'rm {path}/datalad-tests-probe'"],
        stdin=subprocess.PIPE,
        check=True,
    )
    if localpath:
        assert not (Path(localpath) / 'datalad-tests-probe').exists()
