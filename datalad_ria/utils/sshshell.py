from __future__ import annotations

import random
import sys
from pathlib import Path
from typing import Any

from datalad.runner.nonasyncrunner import run_command
from datalad.runner.protocol import GeneratorMixIn
from datalad.runner.coreprotocols import StdOutErrCapture
from queue import Queue


class SSSHShellProtocol(StdOutErrCapture, GeneratorMixIn):
    def __init__(self,
                 done_future: Any = None,
                 encoding: str | None = None) -> None:

        StdOutErrCapture.__init__(
            self,
            done_future=done_future,
            encoding=encoding)
        GeneratorMixIn.__init__(self)

    def timeout(self, fd: int | None) -> bool:
        self.send_result(('timeout', fd))

    def pipe_data_received(self, fd: int, data: bytes) -> None:
        print(f'pipe_data_received({len(data)})', fd, data[:50], file=sys.stderr)
        self.send_result((fd, data))


class SSHShell:
    def __init__(self,
                 host: str,
                 user: str | None = None,
                 identity: str | Path | None = None,
                 timeout: float | None = None
                 ):

        self.stdin_queue = Queue()
        self.current_tag = None
        self.generator = run_command(
            cmd=['ssh']
            + (['-i', str(identity)] if identity is not None else [])
            + [f'{user}@{host}' if user is not None else host],
            protocol=SSSHShellProtocol,
            stdin=self.stdin_queue,
            timeout=timeout)
        self._swallow_login_messages()

    def _swallow_login_messages(self):
        pattern = 'login-end-' + str(random.randint(0, 100000000))
        self.stdin_queue.put(f'echo {pattern}\n'.encode())
        for result in self.generator:
            print(result)
            if result[0] == 1 and result[1] == (pattern + '\n').encode():
                break

    def run_command(self,
                    command: str
                    ):
        self.current_tag = 'datalad-result-' + str(random.randint(0, 1000000000))
        self.current_end_btag = b'\n' + self.current_tag.encode() + b'\n'
        self.current_status_btag = self.current_tag.encode() + b': '
        extended_command = command + f'; x=$?; echo; echo "{self.current_tag}"; echo "{self.current_tag}: $x" >&2\n'
        self.stdin_queue.put(extended_command.encode())

        # Fetch the responses
        seen_stdout = False
        seen_stderr = False
        exit_code = None
        for result in self.generator:
            stdout_data = result[1] if result[0] == 1 else b''
            stderr_data = result[1] if result[0] == 2 else b''
            if self.current_status_btag[1:] in stderr_data:
                exit_code = int(stderr_data[stderr_data.index(self.current_status_btag[1:]) + len(self.current_status_btag[1:]):])
                stderr_data = stderr_data[:stderr_data.index(self.current_status_btag[1:])]
                seen_stderr = True
            if stdout_data.endswith(self.current_end_btag):
                stdout_data = stdout_data[:stdout_data.index(self.current_end_btag)]
                seen_stdout = True
            if stdout_data or stderr_data or exit_code:
                yield {
                    'stdout': stdout_data,
                    'stderr': stderr_data,
                    'status': exit_code
                }
            if seen_stdout and seen_stderr:
                return


ssh_shell = SSHShell(
    host='localhost',
    identity='/home/cristian/.ssh/ria_test_rsa',
    timeout=3)


#print(ssh_shell.run_command('cat /boot/vmlinuz'))

for command in ('ls -l /dsfsdfsd', 'echo -n /xxxxxxxxxxxxx', 'cat /tmp/bbb', 'ls -lH /boot/vmlinuz'):
    print('--------------------:', command)
    for r in ssh_shell.run_command(command):
        print('STDOUT:', r['stdout'])
        print('STDERR:', r['stderr'])
        print('STATUS:', r['status'])
