from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import (
    Path,
    PurePosixPath,
)
from queue import Queue
from threading import Thread
from typing import Any

from annexremote import Master

from datalad_next.runners.iter_subproc import CommandError
from datalad_next.shell import (
    shell,
    ShellCommandExecutor,
    ShellCommandResponseGenerator,
    VariableLengthResponseGenerator,
    VariableLengthResponseGeneratorPosix,
)
from datalad_next.shell.operations.posix import (
    delete,
    download,
    upload,
)

from .riahandler import RIAHandler


lgr = logging.getLogger('datalad.ria.riahandler.ssh')


class OperationBase:
    def __init__(self, args: tuple):
        self.args = args
        self.operation = None


class OperationWithResponseGenerator(OperationBase):
    def __init__(self,
                 args: tuple,
                 response_generator_class: type[ShellCommandResponseGenerator] | None = None,
                 ):
        super().__init__(args)
        self.response_generator_class = response_generator_class


class DownloadOperation(OperationWithResponseGenerator):
    def __init__(self,
                 args: tuple,
                 response_generator_class: type[ShellCommandResponseGenerator] | None = None,
                 ):
        super().__init__(args, response_generator_class)
        self.operation = download


class UploadOperation(OperationBase):
    def __init__(self, args: tuple):
        super().__init__(args)
        self.operation = upload


class DeleteOperation(OperationBase):
    def __init__(self, args: tuple):
        super().__init__(args)
        self.operation = delete


class SshThread(Thread):
    def __init__(self,
                 ssh_command: list[bytes],
                 zero_command_rg_class: type[VariableLengthResponseGenerator] = VariableLengthResponseGeneratorPosix,
                 ):
        super().__init__()
        self.ssh_command = ssh_command
        self.zero_command_rg_class = zero_command_rg_class
        self.command_queue: Queue[tuple[bytes, ShellCommandResponseGenerator | None] | OperationBase | None] = Queue()
        self.result_queue: Queue[ShellCommandResponseGenerator | CommandError] = Queue()

    def run(self) -> None:
        with shell(
                self.ssh_command,
                zero_command_rg_class=self.zero_command_rg_class,
        ) as ssh:
            while True:
                command = self.command_queue.get()
                lgr.debug('SshThread got command: %s', repr(command))
                if command is None:
                    lgr.debug('SshThread exiting')
                    ssh.close()
                    return
                self.result_queue.put(self._process(ssh, command))

    def _process(self,
                 ssh: ShellCommandExecutor,
                 command: tuple[bytes, ShellCommandResponseGenerator | None] | OperationBase
                 ) -> ShellCommandResponseGenerator | CommandError:
        """Process a command that was sent to the shell

        Commands are either bytes-strings, or predefined commands that
        have a convenience implementation, e.g. "download".
        """
        if isinstance(command, tuple):
            return ssh(command[0], response_generator=command[1])
        else:
            try:
                args = (ssh,) + command.args
                if isinstance(command, OperationWithResponseGenerator) \
                        and command.response_generator_class is not None:
                    kwargs = dict(response_generator_class=command.response_generator_class)
                else:
                    kwargs = {}
                return command.operation(*args, **kwargs)
            except CommandError as e:
                return e

    def execute(self,
                command: bytes | str,
                response_generator: ShellCommandResponseGenerator | None = None
                ) -> bytes:
        """Send a command to the processing thread and return the result

        This method collects all output from the remote before returning.
        We do expect only one or two lines of output from the commands.
        """
        if isinstance(command, str):
            command = command.encode()
        self.send((command, response_generator))
        result = self.receive()
        stdout = b''.join(result)
        if result.returncode != 0:
            raise CommandError(
                cmd=f'remote command failed: {command}',
                code=result.returncode,
                stdout=stdout,
                stderr = b''.join(result.stderr_deque)
            )
        return stdout

    def download(self, remote_path: PurePosixPath, local_path: Path):
        self.send(DownloadOperation(args=(remote_path, local_path)))
        result = self.receive()
        if isinstance(result, CommandError):
            raise CommandError from None

    def upload(self, local_path: Path, remote_path: PurePosixPath):
        self.send(UploadOperation(args=(local_path, remote_path)))
        result = self.receive()
        if isinstance(result, CommandError):
            raise CommandError from None

    def send(self, command: Any):
        self.command_queue.put(command)

    def receive(self) -> Any:
        return self.result_queue.get()


@contextmanager
def remote_tempfile(ssh_thread: SshThread) -> PurePosixPath:
    """Create a temporary file on the remote and return its name"""
    temp_file_name = ssh_thread.execute(b'mktemp').strip()
    try:
        yield PurePosixPath(temp_file_name.decode())
    finally:
        ssh_thread.execute(b'rm -f ' + temp_file_name)


class SshRIAHandlerPosix(RIAHandler):
    def __init__(self,
                 annex: Master,
                 dataset_id: str,
                 command_list: list[bytes],
                 remote_path: PurePosixPath,
                 ) -> None:
        super().__init__(annex, dataset_id)
        self.remote_path = remote_path
        self.ssh_thread = SshThread(command_list)
        self.ssh_thread.start()

    def __del__(self):
        # Instruct the ssh-thread to quit and wait for its termination.
        self.ssh_thread.command_queue.put(None)
        self.ssh_thread.join()

    def _get_remote_path(self, key: str) -> PurePosixPath:
        return self.remote_path / self.dataset_id / key

    def prepare(self) -> bool:
        return True

    def transfer_store(self, key: str, local_file: str) -> bool:
        remote_path = self._get_remote_path(key)
        with remote_tempfile(self.ssh_thread) as remote_temp_file:
            # Ensure that the remote path exists.
            self.ssh_thread.execute(f'mkdir -p {remote_path.parent}')

            # Upload the local file to the temporary file.
            self.ssh_thread.upload(Path(local_file), remote_temp_file)

            # Move the temporary file to its final destination.
            # TODO determine the reason for failure from the stderr output
            #  and try to mitigate the problem.
            self.ssh_thread.execute(f'mv {remote_temp_file} {remote_path}')
            return True

    def transfer_retrieve(self, key: str, local_file: str) -> bool:
        self.ssh_thread.download(self._get_remote_path(key), Path(local_file))
        return True

    def remove(self, key: str) -> bool:
        try:
            self.ssh_thread.execute(f'rm -f {self._get_remote_path(key)}')
            return True
        except CommandError:
            return False

    def checkpresent(self, key: str) -> bool:
        try:
            self.ssh_thread.execute(f'test -f {self._get_remote_path(key)}')
            return True
        except CommandError:
            return False
