
from datalad.support.sshconnector import SSHManager as sshman


def test_SSHConnection(ria_sshserver_setup, ria_sshserver):
    # this is the most basic smoke test, login to run a command,
    # check that it does not fail completely.
    # this also verifies that our workaround to enable proper handling on
    # windows, see https://github.com/datalad/datalad-ria/issues/68
    # is still working
    sm = sshman()
    ssh_url = 'ssh://{SSH_LOGIN}@{HOST}:{SSH_PORT}'.format(
        **ria_sshserver_setup)

    con = sm.get_connection(ssh_url)
    out, err = con('ls /')
    assert not err
    assert out
