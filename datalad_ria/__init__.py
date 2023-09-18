"""DataLad demo extension"""

__docformat__ = 'restructuredtext'

import logging
lgr = logging.getLogger('datalad.ria')

# Defines a datalad command suite.
# This variable must be bound as a setuptools entrypoint
# to be found by datalad
command_suite = (
    # description of the command suite, displayed in cmdline help
    "DataLad-ria command suite",
    [
        # specification of a command, any number of commands can be defined
        (
            # importable module that contains the command implementation
            'datalad_ria.create_sibling_ria',
            # name of the command class implementation in above module
            'CreateSiblingRia',
            # optional name of the command in the cmdline API
            'create-sibling-ria2',
            # optional name of the command in the Python API
            'create_sibling_ria2'
        ),
    ]
)

# patch datalad-core
import datalad_ria.patches.enabled

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
