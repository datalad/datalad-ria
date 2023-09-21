from pathlib import PurePosixPath
import pytest

from datalad.distributed.ora_remote import SSHRemoteIO


@pytest.fixture(autouse=False, scope="function")
def ssh_remoteio(ria_sshserver_setup, ria_sshserver):
    ssh = SSHRemoteIO(
        'ssh://{SSH_LOGIN}@{HOST}:{SSH_PORT}'.format(**ria_sshserver_setup)
    )
    yield ssh


def test_SSHRemoteIO_read_file(ssh_remoteio):
    # basic smoke test, just login and use the abstraction to read a file
    etcpasswd = ssh_remoteio.read_file('/etc/passwd')
    # we can assume that the remote /etc/passwd file is not empty
    assert etcpasswd


def test_SSHRemoteIO_handledir(ssh_remoteio, ria_sshserver_setup):
    sshpath = PurePosixPath(ria_sshserver_setup['SSH_PATH'])
    targetdir = sshpath / 'testdir'
    assert not ssh_remoteio.exists(targetdir)
    ssh_remoteio.mkdir(targetdir)
    assert ssh_remoteio.exists(targetdir)
    # place a file in that directory to check the impact of its presence
    # on directory deletion
    targetfpath = targetdir / 'testfile'
    ssh_remoteio.write_file(targetfpath, 'dummy')
    assert ssh_remoteio.exists(targetfpath)

    # XXX calling remove_dir()has no effect, and causes no error!!!
    ssh_remoteio.remove_dir(targetdir)
    assert ssh_remoteio.exists(targetdir)

    # we must "know" that there is content and remove it
    ssh_remoteio.remove(targetfpath)
    assert not ssh_remoteio.exists(targetfpath)

    # XXX remove() cannot do it, we have to use remove_dir()
    # ssh_remoteio.remove(targetdir)
    ssh_remoteio.remove_dir(targetdir)
    assert not ssh_remoteio.exists(targetdir)
