from __future__ import annotations

import logging
import os
import time
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlparse

from annexremote import ProtocolError

from datalad_next.annexremotes import (
    SpecialRemote,
    super_main
)

from .ssh_riahandler import SshRIAHandlerPosix


lgr = logging.getLogger('datalad.ria.ssh')


g_supported_schemes = {
    'ria2+ssh': SshRIAHandlerPosix,
    'ria2+http': None,
    'ria2+https': None,
}


class DemoSshRemote(SpecialRemote):
    def __init__(self, annex: Any):
        super().__init__(annex)
        self.configs = {
            'url': 'url of the RIA store',
            'id': 'ID of the dataset',
        }
        self.url = None
        self.dataset_id = None
        self.handler = None
        # logging and debugging
        self.logfile = open(f'/tmp/ssh-demo-{time.time()}.log', 'wt')
        self.pid = os.getpid()
        self.message(f'__init__(): url: {self.url!r}, dataset_id: {self.dataset_id!r}')
        self.message('__init__() done')

    def __del__(self):
        pass

    def message(self, msg, type='debug'):
        output_msg = f'DemoSshRemote[{self.pid}]: ' + msg
        self.logfile.write(output_msg + '\n')
        self.logfile.flush()
        try:
            self.annex.info('INFO: ' + output_msg)
        except (ProtocolError, AttributeError):
            pass

    def _initialize(self):
        if self.handler is not None:
            return True
        self.url = urlparse(self.annex.getconfig('url'))
        self.dataset_id = self.annex.getconfig('id')
        handler_class = g_supported_schemes.get(self.url.scheme, None)
        if handler_class is None:
            self.message(f'unsupported scheme: {self.url.scheme!r}')
            return False
        self.handler = handler_class(
            self.dataset_id,
            [b'ssh', b'-i', b'/home/cristian/.ssh/gitlab-metadata-key', self.url.netloc.encode()],
            PurePosixPath(self.url.path)
        )
        return True

    def initremote(self):
        self.message(f'initremote called')
        return self._initialize()

    def prepare(self):
        self.message('prepare called')
        if self._initialize() is False:
            return False
        return self.handler.prepare()

    def transfer_store(self, key: str, local_file: str) -> bool:
        return self.handler.transfer_store(key, local_file)

    def transfer_retrieve(self, key: str, local_file: str):
        return self.handler.transfer_retrieve(key, local_file)

    def remove(self, key):
        return self.handler.remove(key)

    def checkpresent(self, key: str):
        return self.handler.checkpresent(key)


def main():
    super_main(
        cls=DemoSshRemote,
        remote_name='demo',
        description='a demo special remote',
    )
