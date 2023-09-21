"""Enable `SSHRemoteIO` operation on Windows

The original code has two problems.

1. The ``cmd``-argument for the shell ssh-process, which is created by:
   ``self.shell = subprocess.Popen(cmd, ...)`` is not correct, if ``self.ssh``i
   is an instance of ``NoMultiplexSSHConnection``.

   The changes in this patch build the correct ``cmd``-argument by adding
   additional arguments to ``cmd``, if `self.ssh` is an instance of
   ``NoMultiplexSSHConnection``. More precisely, the arguments that are
   required to open a "shell" in a ``NoMultiplexSSHConnection`` are stored in
   ``NoMultiplexSSHConnection._ssh_open_args`` and not in
   ``NoMultiplexSSHConnection._ssh_args``. This patch therefore provides
   arguments from both lists, i.e. from ``_ssh_args`` and ``_ssh_open_args`` in
   the call that opens a "shell", if ``self.ssh`` is an instance of
   ``NoMultiplexSSHConnection``.

2. The while-loop that waits to read ``b"RIA-REMOTE-LOGIN-END\\n"`` from the
   shell ssh-process did not contain any error handling. That led to an
   infinite loop in case that the shell ssh-process terminates without writing
   ``b"RIA-REMOTE-LOGIN-END\\n"`` to its stdout, or in the case that the
   stdout-pipeline of the shell ssh-process is closed.

   This patch introduces two checks into the while loop. One check for
   termination of the ssh shell-process, and one check for a closed
   stdout-pipeline of the ssh shell-process, i.e. reading an EOF from the
   stdout-pipeline. If any of those two cases appears, an exception is raised.

In addition, this patch modifies two comments. It adds a missing description of
the ``buffer_size``-parameter of ``SSHRemoteIO.__init__``to the doc-string, and
fixes the description of the condition in the comment on the use of
``DEFAULT_BUFFER_SIZE``.
"""

import logging
import subprocess

from datalad.distributed.ora_remote import ssh_manager
# we need to get this from elsewhere, the orginal code does local imports
from datalad.support.exceptions import CommandError
# we need this for a conditional that is not part of the original code
from datalad.support.sshconnector import NoMultiplexSSHConnection

from datalad_next.utils.consts import COPY_BUFSIZE
from datalad_next.patches import apply_patch

# use same logger as -core
lgr = logging.getLogger('datalad.customremotes.ria_remote')


DEFAULT_BUFFER_SIZE = COPY_BUFSIZE


# The method 'SSHRemoteIO__init__' is a patched version of
# 'datalad/distributed/ora-remote.py:SSHRemoteIO.__init___'
# from datalad@8a145bf432ae8931be7039c97ff602e53813d238
def SSHRemoteIO__init__(self, host, buffer_size=DEFAULT_BUFFER_SIZE):
    """
    Parameters
    ----------
    host : str
      SSH-accessible host(name) to perform remote IO operations
      on.
    buffer_size: int or None
      The preferred buffer size
    """

    # the connection to the remote
    # we don't open it yet, not yet clear if needed
    self.ssh = ssh_manager.get_connection(
        host,
        use_remote_annex_bundle=False,
    )
    self.ssh.open()

    # This is a PATCH: it extends ssh_args to contain all
    # necessary parameters
    ssh_args = self.ssh._ssh_args
    if isinstance(self.ssh, NoMultiplexSSHConnection):
        ssh_args.extend(self.ssh._ssh_open_args)
    cmd = ['ssh'] + ssh_args + [self.ssh.sshri.as_str()]

    # open a remote shell
    self.shell = subprocess.Popen(cmd,
                                  stderr=subprocess.DEVNULL,
                                  stdout=subprocess.PIPE,
                                  stdin=subprocess.PIPE)
    # swallow login message(s):
    self.shell.stdin.write(b"echo RIA-REMOTE-LOGIN-END\n")
    self.shell.stdin.flush()
    while True:
        # This is a PATCH: detect a terminated shell-process
        status = self.shell.poll()
        if status not in (0, None):
            raise CommandError(f'ssh shell process exited with {status}')

        line = self.shell.stdout.readline()
        if line == b"RIA-REMOTE-LOGIN-END\n":
            break

        # This is a PATCH: detect closing of stdout of the shell-process
        if line == '':
            raise RuntimeError(f'ssh shell process close stdout unexpectedly')
    # TODO: Same for stderr?

    # make sure default is used if 0 or None was passed, too.
    self.buffer_size = buffer_size if buffer_size else DEFAULT_BUFFER_SIZE


apply_patch(
    'datalad.distributed.ora_remote', 'SSHRemoteIO', '__init__',
    SSHRemoteIO__init__,
)