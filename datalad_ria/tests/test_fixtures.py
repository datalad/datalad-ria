from .utils import assert_ssh_access


def test_riaserver_fixture(ria_sshserver):
    # we run the same test that the fixture already ran, to verify that
    # the necessary information comes out of the fixture in a usable manner
    assert_ssh_access(
        ria_sshserver['HOST'],
        ria_sshserver['SSH_PORT'],
        ria_sshserver['SSH_LOGIN'],
        ria_sshserver['SSH_SECKEY'],
        ria_sshserver['SSH_PATH'],
        ria_sshserver['LOCALPATH'],
    )
