from datalad.conftest import setup_package

pytest_plugins = [
    "datalad_next.tests.fixtures",
]

from .tests.fixtures import (
    ria_sshserver,
    ria_sshserver_setup,
    common_init_opts,
)
