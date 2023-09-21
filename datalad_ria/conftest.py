from datalad.conftest import setup_package

pytest_plugins = [
    "datalad_next.tests.fixtures",
]

from .tests.fixtures import (
    common_ora_init_opts,
    populated_dataset,
    ria_sshserver,
    ria_sshserver_setup,
)

from .tests.test_ssh_remote_io import (
    ssh_remote_wdir,
    ssh_remoteio,
)