from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from datalad.tests.utils_pytest import (
    on_windows,
    skip_if,
)
from datalad_next.runners import CommandError
from datalad_next.shell import shell
from datalad_next.shell.tests.test_shell import _get_cmdline

from ..ssh_riahandler import SshRIAHandlerPosix


def test_download_basics():
    class Dummy:
        def __init__(self):
            self.stderr_deque = None

    def progress_callback(size, total_size):
        print(size, total_size)


@skip_if(on_windows)
def test_ensure_writable(sshserver, monkeypatch):
    ssh_url, local_path = sshserver
    ssh_args, ssh_path = _get_cmdline(ssh_url)
    # Create a write protected directory and file on the filesystem that is
    # accessible via ssh
    (local_path / 'd1').mkdir()
    (local_path / 'd1' / 'f1').write_text('content', encoding='utf-8')
    (local_path / 'd1' / 'f1').chmod(0o444)
    (local_path / 'd1').chmod(0o555)

    # We know the ssh-server is on a POSIX system
    ssh_path = Path(ssh_path) / 'd1' / 'f1'
    with shell(ssh_args) as ssh_executor:
        # Ensure that removing does not work
        with pytest.raises(CommandError) as e:
            r = ssh_executor(f'rm {ssh_path.as_posix()}')
            tuple(r)

        # Create an "empty" `ssh_riahandler`. It will provide the ssh-thread
        # and the `_ensure_writable`-context.
        remote_mock = MagicMock()
        remote_mock.annex = MagicMock()
        ria_handler = SshRIAHandlerPosix(
            remote_mock,
            local_path,
            '0123456-789a-bcde-f012-3456789abcde',
            ssh_args
        )

        with ria_handler._ensure_writable(ssh_path.parent):
            r = ssh_executor(f'rm {ssh_path.as_posix()}')
            tuple(r)

        # Delete the ria-handler, that will stop the ssh-thread and allow the
        # interpreter to exit.
        del ria_handler
