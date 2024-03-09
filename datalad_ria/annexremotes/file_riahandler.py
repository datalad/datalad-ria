from __future__ import annotations

import logging
import os
import shutil
from pathlib import (
    PurePath,
)
from threading import (
    Lock,
)

from datalad_next.annexremotes import SpecialRemote, RemoteError

from .riahandler import RIAHandler


lgr = logging.getLogger('datalad.ria.riahandler.file')


class FileRIAHandlerPosix(RIAHandler):
    def __init__(self,
                 special_remote: SpecialRemote,
                 base_path: PurePath,
                 dataset_id: str,
                 ) -> None:
        super().__init__(special_remote, base_path, dataset_id)
        self.debug = special_remote.annex.debug
        self.base_path = base_path
        self.command_lock = Lock()

    def initremote(self):
        dataset_path = self.base_path / self.dataset_id
        lgr.debug(f'initremote: making dir: {dataset_path}')
        os.makedirs(self.base_path / self.dataset_id, exist_ok=True)

    def prepare(self):
        dataset_path = self.base_path / self.dataset_id
        lgr.debug(f'prepare: making dir: {dataset_path}')
        os.makedirs(self.base_path / self.dataset_id, exist_ok=True)

    def transfer_store(self, key: str, local_file: str):
        with self.command_lock:
            # If we already have the key, we will not transfer anything
            if not self._locked_checkpresent(key):
                try:
                    transfer_path = self.get_ria_path(key, '.transfer')
                    destination_path = self.get_ria_path(key)
                    shutil.copy(local_file, transfer_path)
                    shutil.move(transfer_path, destination_path)
                except OSError as e:
                    shutil.rmtree(transfer_path, ignore_errors=True)
                    raise RemoteError(
                        f'Failed to store key {key} in special remote'
                    ) from e

    def transfer_retrieve(self, key: str, local_file: str):
        with self.command_lock:
            if not self._locked_checkpresent(key):
                raise RemoteError(f'Key {key} not present in special remote')
            shutil.copy(self.get_ria_path(key), local_file)

    def remove(self, key: str):
        with self.command_lock:
            if self._locked_checkpresent(key):
                try:
                    shutil.move(
                        self.get_ria_path(key),
                        self.get_ria_path(key, '.deleted')
                    )
                except OSError as e:
                    raise RemoteError(
                        f'Failed to remove key {key} from special remote'
                    ) from e

    def checkpresent(self, key: str) -> bool:
        with self.command_lock:
            return self._locked_checkpresent(key)

    def _locked_checkpresent(self, key: str) -> bool:
        try:
            os.stat(self.get_ria_path(key))
        except OSError as e:
            lgr.debug(
                f'_locked_checkpresent: os.stat({self.get_ria_path(key)}) '
                f'failed, reason: {e!r}'
            )
            return False
        return True
