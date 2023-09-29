from __future__ import annotations

import logging
import random
from pathlib import Path
from typing import Any

from datalad.runner.nonasyncrunner import run_command
from datalad.runner.protocol import GeneratorMixIn
from datalad.runner.coreprotocols import StdOutErrCapture
from queue import Queue


lgr = logging.getLogger('datalad.ria.sshshell')


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
        lgr.log(
            5,
            'SSHShellProtocol: pipe_data_received(%d): %s, %s',
            len(data), fd, repr(data)
        )
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
        for length in range(min(len(data_chunk), len(self.pattern)), 0, -1):
            # We only have to check pattern parts
            # that are not longer than the data chunk
            active_pattern = self.pattern[0:length]
            if active_pattern in data_chunk:
                return data_chunk.index(active_pattern), length
        return -1, -1

    def _find_pattern_tail(self, data_chunk: bytes) -> int:
        # Because we have seen the start of the pattern, we are looking for the
        # remaining patter at the beginning od this chunk.
        active_pattern = self.pattern[self.state:self.state + len(data_chunk)]
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

    def _get_tag(self, prefix: bytes = b'datalad-result-'):
        return prefix + str(random.randint(0, 1000000000)).encode()

    def _swallow_login_messages(self):
        pattern = self._get_tag(b'login-end-') + b'\n'
        self.stdin_queue.put(b'echo ' + pattern)
        pattern_filter = PatternFilter(pattern)
        for (file_number, data) in self.generator:
            if file_number == 1:
                data, pattern_found, remainder = pattern_filter.filter(data)
                if pattern_found is True:
                    break

    def run_command(self,
                    command: str
                    ):
        current_tag = self._get_tag()
        current_status_tag = current_tag + b':'
        extended_command = (
                command
                + f'; x=$?; echo -n "{current_tag.decode()}";'
                + f'echo "{current_status_tag.decode()}$x" >&2\n'
        )
        self.stdin_queue.put(extended_command.encode())

        # Fetch the responses
        seen_stdout = False
        seen_stderr = False
        exit_code = None
        stdout_pfilter = PatternFilter(current_tag)
        stderr_pfilter = PatternFilter(current_status_tag)
        newline_filter = PatternFilter(b'\n')
        exit_code_data = b''
        stderr_tag_found = False
        for result in self.generator:
            stdout_data = result[1] if result[0] == 1 else b''
            stderr_data = result[1] if result[0] == 2 else b''

            if stderr_data:
                if stderr_tag_found is False:
                    stderr_data, stderr_tag_found, newline_data = stderr_pfilter.filter(stderr_data)
                    stderr_data = newline_data

                if stderr_tag_found is True:
                    stderr_data, newline_found, _ = newline_filter.filter(stderr_data)
                    # Assemble result code
                    exit_code_data += stderr_data
                    # Set stderr_data to b'' to prevent the exit code from
                    # appearing in the stderr output.
                    stderr_data = b''
                    if newline_found:
                        exit_code = int(exit_code_data)
                        seen_stderr = True

            if stdout_data:
                stdout_data, stdout_tag_found, stdout_remainder = stdout_pfilter.filter(stdout_data)
                if stdout_tag_found:
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

    lgr.setLevel(5)
    for command in ('ls -l /dsfsdfsd', 'echo -n /xxxxxxxxxxxxx', 'cat /tmp/bbb', 'ls -lH /boot/vmlinuz'):
        print('--------------------:', command)
        for r in ssh_shell.run_command(command):
            print('STDOUT:', r['stdout'])
            print('STDERR:', r['stderr'])
            print('STATUS:', r['status'])


if __name__ == '__main__':
    main()
