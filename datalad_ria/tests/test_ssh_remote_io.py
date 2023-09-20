from datalad.distributed.ora_remote import SSHRemoteIO


def test_SSHRemoteIO_basics(ria_sshserver_setup, ria_sshserver):
    # basic smoke test, just login and use the abstraction to read a file
    ssh = SSHRemoteIO(
        'ssh://{SSH_LOGIN}@{HOST}:{SSH_PORT}'.format(**ria_sshserver_setup)
    )
    etcpasswd = ssh.read_file('/etc/passwd')
    # we can assume that the remote /etc/passwd file is not empty
    assert etcpasswd
