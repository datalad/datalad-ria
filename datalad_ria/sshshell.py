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


class PatternFilter:
    """ A class to filter a pattern from a stream of bytes

    Instances of this class filter a pattern from a stream of bytes. The byte
    stream might be split in different chunks. If the pattern is detected,
    the filter will indicate this, filter out the pattern and returns what
    remains from the last chunk.
    Each chunk is handed to PatternFilter.filter(), which returns the bytes
    without the pattern.
    """
    def __init__(self, pattern: bytes):
        self.pattern = pattern
        self.state = 0
        self.remainder = bytearray()

    def _find_pattern_start(self, data_chunk: bytes) -> tuple[int, int]:
        # Because the pattern might be only partially in the data chunk, we
        # have to check for all prefixes.
        for length in range(min(len(data_chunk), len(self.pattern)) + 1, 0, -1):
            # We only have to check pattern parts
            # that are not longer than the data chunk
            print(length)
            active_pattern = self.pattern[0:length]
            if active_pattern in data_chunk:
                return data_chunk.index(active_pattern), length
        return -1, -1

    def _find_pattern_tail(self, data_chunk: bytes) -> int:
        # Because we have seen the start of the pattern, we are looking for the
        # remaining patter at the beginning od this chunk.
        active_pattern = self.pattern[self.state:self.state + len(data_chunk)]
        print(active_pattern)
        if data_chunk.startswith(active_pattern):
            return min(len(data_chunk), len(self.pattern) - self.state)
        return -1

    def filter(self, data_chunk: bytes) -> tuple[bytes, bool, bytes]:
        assert isinstance(data_chunk, bytes) and data_chunk != b''
        while True:
            if self.state == 0:
                # The pattern could only be partially existing in the buffer.
                # and it would match at most buffer-size bytes.
                index, length = self._find_pattern_start(data_chunk)
                if index >= 0:
                    if length < len(self.pattern):
                        # The pattern start was found
                        self.state = length
                        return data_chunk[:index], False, b''
                    else:
                        # The complete pattern was found
                        return data_chunk[:index], True, data_chunk[index + length:]
                # Nothing was found, return the complete chunk
                return data_chunk, False, b''
            else:
                length = self._find_pattern_tail(data_chunk)
                if length < 0:
                    # The tail was not found, reset the state look for the pattern
                    # in the remainder of the data chunk.
                    self.state = 0
                    continue
                elif length == len(self.pattern) - self.state:
                    # The pattern was found
                    self.state = 0
                    return b'', True, data_chunk[length:]
                else:
                    # The chunk was shorter than the remaining pattern, continue
                    # the tail matching
                    self.state += length
                    return b'', False, b''


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


def main():
    ssh_shell = SSHShell(
        host='localhost',
        identity='/home/cristian/.ssh/ria_test_rsa',
        timeout=3)

    for command in ('ls -l /dsfsdfsd', 'echo -n /xxxxxxxxxxxxx', 'cat /tmp/bbb', 'ls -lH /boot/vmlinuz'):
        print('--------------------:', command)
        for r in ssh_shell.run_command(command):
            print('STDOUT:', r['stdout'])
            print('STDERR:', r['stderr'])
            print('STATUS:', r['status'])


if __name__ == '__main__':
    main()
