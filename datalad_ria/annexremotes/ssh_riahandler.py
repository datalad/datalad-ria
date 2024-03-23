from __future__ import annotations

import logging
import traceback
from contextlib import contextmanager
from functools import partial
from pathlib import (
    Path,
    PurePosixPath,
)
from queue import Queue
from threading import (
    Thread,
    Lock,
)
from typing import (
    Any,
    Callable,
)

from datalad.support.annex_utils import _sanitize_key
from datalad_next.annexremotes import (
    RemoteError,
    SpecialRemote,
)
from datalad_next.runners.iter_subproc import (
    CommandError,
    OutputFrom,
)
from datalad_next.shell import (
    shell,
    ShellCommandExecutor,
    ShellCommandResponseGenerator,
    VariableLengthResponseGenerator,
    VariableLengthResponseGeneratorPosix,
)
from datalad_next.shell.operations.posix import (
    DownloadResponseGeneratorPosix,
    delete,
    download,
    upload,
)

from .riahandler import RIAHandler


lgr = logging.getLogger('datalad.ria.riahandler.ssh')

layout_version = '1'


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


class DownloadProgressRG(DownloadResponseGeneratorPosix):
    """A POSIX download class that supports progress callback"""
    def __init__(self,
                 stdout: OutputFrom,
                 progress_callback: Callable[[int, int], None]
                 ) -> None:
        super().__init__(stdout)
        self.progress_callback = progress_callback

    def send(self, _) -> bytes:
        # TODO: this is basically a verbatim copy of
        #  `DownloadResponseGenerator.send`, which is augmented with
        #  progress-callback invocations. The code is replicated to keep the
        #  performance of `DownloadResponseGenerator.send` high
        #  by avoiding the overhead of checking for a non-None
        #  `self.progress_callback` attribute after every read operation.
        chunk = b''
        while True:
            if self.state == 2:
                if not chunk:
                    chunk = next(self.stdout_gen)
                self.read += len(chunk)
                self.progress_callback(self.read, self.length)
                if self.read >= self.length:
                    self.state = 3
                    excess = self.read - self.length
                    if excess > 0:
                        chunk, self.returncode_chunk = chunk[:-excess], chunk[-excess:]
                    else:
                        self.returncode_chunk = b''
                    if chunk:
                        return chunk
                else:
                    return chunk

            if self.state == 1:
                self.length, chunk = self._get_number_and_newline(
                    b'',
                    self.stdout_gen,
                )
                # a negative length indicates an error during download length
                # determination or download length-communication.
                if self.length < 0:
                    self.state = 1
                    self.returncode = 23
                    raise StopIteration
                self.state = 2
                continue

            if self.state == 3:
                self.returncode, trailing = self._get_number_and_newline(
                    self.returncode_chunk,
                    self.stdout_gen,
                )
                if trailing:
                    lgr.warning(
                        'unexpected output after return code: %s',
                        repr(trailing))
                self.state = 4

            if self.state == 4:
                self.state = 1
                self.check_result()
                raise StopIteration

            raise RuntimeError(f'unknown state: {self.state}')


# A factory for `DownloadProgressRG`-instances. This is used together with
# `partial` to create a callable with a single `stdout`-argument, that creates
# instance of `DownloadProgressRG`, This partial can then be used in
# `shell.posix.download`.
# Why so complicated? Because the operation `shell.posix.download` is optimized
# for speed. Therefore, it does not support an optional `progress_callback`.
# To keep the `shell.posix.download`-interface simple, it does neither support
# a progress callback, nor does it support to pass keyword-arguments to the
# response generator that it creates.
# To support progress callbacks nevertheless, we create a new response
# generator class, which supports progress callbacks, i.e. `DownloadProgressRG`
# and parameterize its instances with the appropriate callback.
def create_download_rg_class(progress_callback: Callable[[int, int], None],
                             stdout: OutputFrom,
                             ) -> DownloadResponseGeneratorPosix:
    return DownloadProgressRG(stdout, progress_callback)


class SshThread(Thread):
    """A thread that accepts commands and executes them in a persistent shell

    We use a `datalad_next.shell.shell`-context to communicate with persistent
    remote shells. This thread creates such a context and executes commands
    that are sent via a queue in this context.
    """
    def __init__(self,
                 ssh_command: list[bytes],
                 zero_command_rg_class: type[VariableLengthResponseGenerator] = VariableLengthResponseGeneratorPosix,
                 ):
        super().__init__()
        self.ssh_command = ssh_command
        self.zero_command_rg_class = zero_command_rg_class
        self.command_queue: Queue[tuple[bytes, ShellCommandResponseGenerator | None] | OperationBase | None] = Queue()
        self.result_queue: Queue[ShellCommandResponseGenerator | CommandError] = Queue()
        self.run_lock = Lock()

    def run(self) -> None:
        with shell(
                self.ssh_command,
                zero_command_rg_class=self.zero_command_rg_class,
        ) as ssh:
            try:
                while True:
                    command = self.command_queue.get()
                    lgr.debug(
                        'SshThread: got command: %s',
                        str(command)
                    )
                    if command is None:
                        lgr.debug('SshThread: exit requested')
                        return
                    self.result_queue.put(self._process(ssh, command))
            finally:
                ssh.close()

    def _process(self,
                 ssh: ShellCommandExecutor,
                 command: tuple[bytes, ShellCommandResponseGenerator | None] | OperationBase
                 ) -> ShellCommandResponseGenerator | CommandError:
        """Process a command in the persistent shell

        Commands are either bytes-strings, or predefined commands that
        have a convenience implementation, e.g. "download".
        """
        try:
            if isinstance(command, tuple):
                return ssh(command[0], response_generator=command[1])
            else:
                args = (ssh,) + command.args
                if (
                    isinstance(command, OperationWithResponseGenerator)
                    and command.response_generator_class is not None
                ):
                    kwargs = dict(response_generator_class=command.response_generator_class)
                else:
                    kwargs = {}
                return command.operation(*args, **kwargs)
        except CommandError as e:
            lgr.debug('SshThread: command failed: %s (code: %s):\n%s',
                      str(command),
                      str(e.code),
                      traceback.format_exc())
            return e
        except BaseException as e:
            lgr.debug(
                'SshThread: unexpected exception during '
                'execution of command: %s:\n%s',
                str(command),
                traceback.format_exc(),
            )
            return CommandError(
                f'SshThread: unexpected exception: '
                ''.join(traceback.format_exception(e))
            )

    def execute(self,
                command: bytes | str,
                response_generator: ShellCommandResponseGenerator | None = None
                ) -> bytes:
        """Send a command to the processing thread and return the result

        This method collects all output from the remote before returning.
        We do expect only one or two lines of output from the commands.
        """
        with self.run_lock:
            # Send the command to the ssh-thread and wait for the result.
            if isinstance(command, str):
                command = command.encode()
            self.send((command, response_generator))
            result = self.receive()
            self._check_for_command_error(command, result)

            # Check the return value of the remote operation. Raise a
            # `RemoteError` if the return code is non-zero.
            stdout = b''.join(result)
            if result.returncode != 0:
                message = (
                    f'command failed: {command}, '
                    f'[code: {result.returncode}] '
                    f'[stdout: {stdout!r}] '
                    f'[stderr: {b"".join(result.stderr_deque)!r}]'
                )
                raise RemoteError(message)
            return stdout

    def download(self,
                 remote_path: PurePosixPath,
                 local_path: Path,
                 progress_handler: Callable[[int, int], None] | None = None
                 ):
        """A simple wrapper around `datalad_next.shell.posix.download` """
        with self.run_lock:
            self.send(
                DownloadOperation(
                    args=(remote_path, local_path, progress_handler)))
            result = self.receive()
            self._check_for_command_error('<download>', result)

    def upload(self,
               local_path: Path,
               remote_path: PurePosixPath,
               progress_handler: Callable[[int, int], None] | None = None
               ):
        """A simple wrapper around `datalad_next.shell.posix.upload` """
        with self.run_lock:
            self.send(
                UploadOperation(
                    args=(local_path, remote_path, progress_handler)))
            result = self.receive()
            self._check_for_command_error('<upload>', result)

    def send(self, command: Any):
        self.command_queue.put(command)

    def receive(self) -> Any:
        return self.result_queue.get()

    @staticmethod
    def _check_for_command_error(command, result):
        # If the result is an exception, raise it in this thread as a
        # RemoteError. Even if our caller doesn't catch it, it still
        # indicates an error to the protocol handler.
        if isinstance(result, CommandError):
            message = f'command failed: {command}, exception: {result}'
            lgr.error(message)
            raise RemoteError(message) from result


class SshRIAHandlerPosix(RIAHandler):
    def __init__(self,
                 special_remote: SpecialRemote,
                 base_path: PurePosixPath,
                 dataset_id: str,
                 command_list: list[bytes],
                 ) -> None:
        super().__init__(special_remote, base_path, dataset_id)
        self.command_list = command_list
        self.ssh_thread = None
        self.debug = special_remote.annex.debug
        self.base_path = base_path
        self.command_lock = Lock()
        self.layout_version = None
        self._initialize()

    def __del__(self):
        # Instruct the ssh-thread to quit and wait for its termination.
        self.ssh_thread.command_queue.put(None)
        self.ssh_thread.join()

    def _initialize(self):
        if self.ssh_thread is None:
            lgr.debug(
                '_initialize: starting ssh_thread for command %s',
                str(self.command_list),
            )
            self.ssh_thread = SshThread(self.command_list)
            self.ssh_thread.start()

        # Ensure that the base path exists
        self.ssh_thread.execute(f'mkdir -p {self.get_ria_repo_path()}')
        # Get the layout version
        self.layout_version = self._get_layout_version()

    def _get_layout_version(self) -> str:
        try:
            return self.ssh_thread.execute(
                f'cat {self.base_path / "ria-layout-version"}'
            ).strip().decode()
        except CommandError:
            self.ssh_thread.execute(
                f'echo {layout_version} > '
                f'{self.base_path / "ria-layout-version"}'
            )
            return layout_version

    def transfer_store(self, key: str, local_file: str) -> None:
        key = _sanitize_key(key)

        with self.command_lock:
            # Only transfer, if we don't have the key yet
            if self._locked_checkpresent(key):
                return

            with self._remote_tempfile() as remote_temp_file:
                # Upload the local file to the temporary file.
                self.ssh_thread.upload(
                    Path(local_file),
                    remote_temp_file,
                    self.progress_handler
                )

                # Ensure that the remote path exists.
                final_path = self.get_ria_key_path(key)
                self.ssh_thread.execute(f'mkdir -p {final_path.parent}')

                # Ensure that the remote path is writable.
                with self._ensure_writable(final_path.parent):
                    # Move the temporary file to its final destination.
                    self.ssh_thread.execute(
                        f'mv -f {remote_temp_file} {final_path}'
                    )

    def transfer_retrieve(self, key: str, local_file: str) -> None:
        key = _sanitize_key(key)
        with self.command_lock:
            if self._locked_checkpresent(key):
                source = self.get_ria_key_path(key)
                destination = Path(local_file)
                self.ssh_thread.download(
                    source,
                    destination,
                    partial(
                        create_download_rg_class,
                        self.progress_handler,
                    ),
                )
                return
            raise RemoteError(f'key {key} not present')

    @contextmanager
    def _ensure_writable(self, path: PurePosixPath):
        mode = self.ssh_thread.execute(f'ls -ldn {path}').decode().split()[0]
        if mode[0] not in ('-', 'd'):
            raise RemoteError(f'_ensure writable called on file type {mode[0]}')
        modification_required = mode[2] != 'w'
        if modification_required:
            self.ssh_thread.execute(f'chmod u+w {path}')
        try:
            yield
        finally:
            if modification_required:
                self.ssh_thread.execute(f'chmod u-w {path}')

    @contextmanager
    def _remote_tempfile(self) -> PurePosixPath:
        """Create a temporary file on the remote and return its name"""
        try:
            temp_file_name = self.ssh_thread.execute(b'mktemp').strip().decode()
        except CommandError as e:
            lgr.debug(
                '@contextmanager: SshRIAHandlerPosix._remote_tempfile: '
                'failed to create temporary file on remote due to exception: %s',
                ''.join(traceback.format_exception(e))
            )
            raise RemoteError('could not create remote temp file') from e
        try:
            yield PurePosixPath(temp_file_name)
        finally:
            try:
                self.ssh_thread.execute(f'rm -f {temp_file_name}')
            except CommandError as e:
                lgr.debug(
                    '@contextmanager: SshRIAHandlerPosix._remote_tempfile: '
                    'failed to remove temporary file %s on remote due to '
                    'exception: %s',
                    ''.join(traceback.format_exception(e)),
                )
                raise RemoteError('could not remove remote temp file') from e

    def remove(self, key: str) -> None:
        key = _sanitize_key(key)
        with self.command_lock:
            if self._locked_checkpresent(key):
                self._locked_remove(key)

    def _locked_remove(self, key: str) -> None:
        key_path = self.get_ria_key_path(key)
        with self._ensure_writable(key_path.parent):
            self.ssh_thread.execute(f'rm -rf {key_path.parent}')

    def checkpresent(self, key: str) -> bool:
        key = _sanitize_key(key)
        with self.command_lock:
            return self._locked_checkpresent(key)

    def _locked_checkpresent(self, key: str) -> bool:
        try:
            self.ssh_thread.execute(f'test -f {self.get_ria_key_path(key)}')
            return True
        except CommandError:
            return False
