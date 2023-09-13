from datalad_ria.tests.utils import assert_ssh_access

# we import this one, rather than putting it into conftest.py, because
# it is considered internal, in contrast to `ria_sshserver`
from datalad_ria.tests.fixtures import ria_sshserver_setup


def test_riaserver_setup_fixture(ria_sshserver_setup):
    # we run the same test that the fixture already ran, to verify that
    # the necessary information comes out of the fixture in a usable manner
    assert_ssh_access(
        ria_sshserver_setup['HOST'],
        ria_sshserver_setup['SSH_PORT'],
        ria_sshserver_setup['SSH_LOGIN'],
        ria_sshserver_setup['SSH_SECKEY'],
        ria_sshserver_setup['SSH_PATH'],
        ria_sshserver_setup['LOCALPATH'],
    )


def test_riaserver_fixture(ria_sshserver):
    # base url and local path
    assert len(ria_sshserver) == 2
    assert ria_sshserver[0].startswith('ria+ssh://')


def test_populate_dataset_fixture(populated_dataset):
    # populated_dataset is a Dataset object
    assert populated_dataset.pathobj.exists()
    assert (populated_dataset.pathobj / 'three.txt').exists()
    with open(populated_dataset.pathobj / 'three.txt') as file:
        payload = file.read()
        assert payload == "content3"
    assert (populated_dataset.pathobj / 'subdir').is_dir()
