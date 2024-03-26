from datalad.conftest import setup_package

pytest_plugins = [
    "datalad_next.tests.fixtures",
]

from .tests.fixtures import (
    common_ora_init_opts,
    populated_dataset,
    ria_server_localpath,
    ria_sshserver,
    ria_sshserver_setup,
    ria_store_localaccess,
)
