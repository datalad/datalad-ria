from __future__ import annotations

import os
import shutil
import time
from pathlib import Path
from typing import Any

from datalad_next.annexremotes import super_main
from datalad_next.annexremotes.uncurl import UncurlRemote


class DemoRemote(UncurlRemote):

    def __init__(self, annex: Any):
        super().__init__(annex)
        self.store_path = Path('/home/cristian/tmp/demo-store')
        self.tmp_path = self.store_path / 'transfer'
        self.store_path.mkdir(parents=True, exist_ok=True)
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.logfile = Path(f'/tmp/ora-demo-{time.time()}.log').open('wt')

    def __del__(self):
        self.logfile.close()

    def message(self, msg, type='debug'):
        self.logfile.write(msg + '\n')
        self.logfile.flush()

    def key_path(self, key: str) -> Path:
        return self.store_path / key

    def tmp_key_path(self, key: str) -> Path:
        return self.tmp_path / (key + str(time.time()) + '.transfering')

    def initremote(self):
        pass

    def prepare(self):
        pass

    def claimurl(self, url):
        return url.startswith('demo://')

    def checkurl(self, url):
        return url.startswith('demo://')

    def transfer_store(self, key: str, local_file: str):
        # We transfer via a temporary unique name in order
        # prevent checkpresent from reporting while we are
        # uploading the file.
        transfer_path = self.tmp_key_path(key)
        shutil.copy(local_file, transfer_path)
        shutil.move(transfer_path, self.key_path(key))

    def transfer_retrieve(self, key: str, local_file: str):
        shutil.copy(self.key_path(key), local_file)

    def checkpresent(self, key: str):
        return self.key_path(key).exists()

    def remove(self, key):
        os.unlink(self.key_path(key))

    def whereis(self, key):
        return str(self.store_path / key)

    def getavailability(self):
        return 'local'


def main():
    """cmdline entry point"""
    super_main(
        cls=DemoRemote,
        remote_name='demo',
        description='a demo special remote',
    )
