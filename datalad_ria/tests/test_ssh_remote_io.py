from pathlib import PurePosixPath
import pytest

from datalad.distributed.ora_remote import SSHRemoteIO


@pytest.fixture(autouse=False, scope="function")
def ssh_remoteio(ria_sshserver_setup, ria_sshserver):
    """Yield a ``SSHRemoteIO`` instance matching the RIA server setup"""
    ssh = SSHRemoteIO(
        'ssh://{SSH_LOGIN}@{HOST}:{SSH_PORT}'.format(**ria_sshserver_setup)
    )
    yield ssh


@pytest.fixture(autouse=False, scope="function")
def ssh_remote_wdir(ssh_remoteio, ria_sshserver_setup):
    """Like ``ssh_remoteio``, but also yields a working directory

    It used ``mktemp -d`` on the remote end, and also remove the working
    directory at the end.
    """
    sshpath = PurePosixPath(ria_sshserver_setup['SSH_PATH'])
    out, err = ssh_remoteio.ssh(f'mktemp -d {sshpath}/sshremotewdir.XXXXXXXX')
    # hopefully catch more stupid errors of unexpectedness
    assert '/sshremotewdir' in out
    wdir = PurePosixPath(out.rstrip('\n'))
    yield ssh_remoteio, wdir
    # clean up
    # only run dangerous 'rm -rf' if needed
    if ssh_remoteio.exists(wdir):
        ssh_remoteio.ssh(f'echo rm -rf "{wdir}"')


def test_SSHRemoteIO_read_file(ssh_remoteio):
    # basic smoke test, just login and use the abstraction to read a file
    etcpasswd = ssh_remoteio.read_file('/etc/passwd')
    # we can assume that the remote /etc/passwd file is not empty
    assert etcpasswd


# this is not using `ssh_remote_wdir`, because we want to go manual
# on all steps and have things break in a test, and not in a fixture
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


def test_SSHRemoteIO_symlink(ssh_remote_wdir):
    ssh_remoteio, targetdir = ssh_remote_wdir
    targetfpath = targetdir / 'testfile'
    assert not ssh_remoteio.exists(targetfpath)
    ssh_remoteio.symlink('/etc/passwd', targetfpath)
    assert ssh_remoteio.exists(targetfpath)
    assert ssh_remoteio.read_file('/etc/passwd') \
        == ssh_remoteio.read_file(targetfpath)
    # verify that we can remove a symlink
    ssh_remoteio.remove(targetfpath)
    assert not ssh_remoteio.exists(targetfpath)
