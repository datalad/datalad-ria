from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any

from datalad_next.annexremotes import super_main
from datalad_next.annexremotes.uncurl import UncurlRemote


class DemoRemote(UncurlRemote):

    def __init__(self, annex: Any):
        super().__init__(annex)
        self.store_path = Path('/home/cristian/tmp/demo-store')
        self.store_path.mkdir(parents=True, exist_ok=True)

    def key_path(self, key) -> Path:
        return self.store_path / key

    def initremote(self):
        pass

    def prepare(self):
        pass

    def claimurl(self, url):
        return url.startswith('demo://')

    def checkurl(self, url):
        return url.startswith('demo://')

    def transfer_store(self, key: str, local_file: str):
        shutil.copy(local_file, self.key_path(key))

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
