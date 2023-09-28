from pathlib import PurePosixPath
import pytest

from datalad.support.exceptions import CommandError

from datalad.distributed.ora_remote import (
    RemoteCommandFailedError,
    SSHRemoteIO,
)


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


def test_SSHRemoteIO_readwrite_file(ssh_remote_wdir):
    ssh_remoteio, targetdir = ssh_remote_wdir
    # basic smoke test, just login and use the abstraction to read a file
    etcpasswd = ssh_remoteio.read_file('/etc/passwd')
    # we can assume that the remote /etc/passwd file is not empty
    assert etcpasswd
    # from scratch
    content = 'nonewlines'
    targetfilepath = targetdir / 'testfile'
    ssh_remoteio.write_file(targetfilepath, content)
    assert ssh_remoteio.exists(targetfilepath)
    # XXX https://github.com/datalad/datalad-ria/issues/89
    assert ssh_remoteio.read_file(targetfilepath) == f'{content}\n'
    ssh_remoteio.ssh(f'printf "%s" "{content}" > {targetfilepath}')
    # XXX this is https://github.com/datalad/datalad-ria/issues/86
    # which hangs due to https://github.com/datalad/datalad-ria/issues/87
    assert ssh_remoteio.read_file(targetfilepath) == content


def test_SSHRemoteIO_get_7z(ssh_remoteio):
    # smoke test
    have_7z = ssh_remoteio.get_7z()
    if have_7z:
        # we must be able to find it too
        assert '7z' in ssh_remoteio.ssh('which 7z')[0]
    else:
        with pytest.raises(CommandError):
            ssh_remoteio.ssh('which 7z')


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

    with pytest.raises(RemoteCommandFailedError):
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


def test_SSHRemoteIO_updownload(ssh_remote_wdir, tmp_path):
    ssh_remoteio, targetdir = ssh_remote_wdir
    probefpath = tmp_path / 'probe'
    content = 'dummy'
    probefpath.write_text(content)

    def noop_callback(val):
        pass

    ssh_remoteio.put(
        probefpath, targetdir / 'myupload', noop_callback)
    assert ssh_remoteio.exists(targetdir / 'myupload')
    ssh_remoteio.get(
        targetdir / 'myupload', tmp_path / 'mydownload', noop_callback)
    assert (tmp_path / 'mydownload').read_text() == content

    # rename and redownload
    renamed_targetfpath = targetdir / 'probe'
    ssh_remoteio.rename(targetdir / 'myupload', renamed_targetfpath)
    ssh_remoteio.get(
        renamed_targetfpath, tmp_path / 'mydownload2', noop_callback)
    assert (tmp_path / 'mydownload').read_text() \
        == (tmp_path / 'mydownload2').read_text()

    # redo upload, overwriting the renamed file
    probefpath.write_text('allnew')
    ssh_remoteio.put(
        probefpath, renamed_targetfpath, noop_callback)
    assert ssh_remoteio.exists(renamed_targetfpath)
    assert ssh_remoteio.read_file(renamed_targetfpath) == 'allnew'
