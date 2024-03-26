from __future__ import annotations

import logging
from abc import (
    ABCMeta,
    abstractmethod,
)
from pathlib import (
    PurePath,
    PurePosixPath,
)

from datalad.customremotes import SpecialRemote


lgr = logging.getLogger('datalad.ria.riahandler')


class RIAHandler(metaclass=ABCMeta):
    def __init__(self,
                 special_remote: SpecialRemote,
                 base_path: PurePath,
                 dataset_id: str,
                 ) -> None:
        self.special_remote = special_remote
        self.base_path = PurePosixPath(base_path)
        self.dataset_id = dataset_id
        self.annex = special_remote.annex

    def get_ria_repo_path(self) -> PurePosixPath:
        return (
            self.base_path
            / self.dataset_id[:3] / self.dataset_id[3:]
        )

    def get_ria_key_path(self,
                         key: str,
                         extension: str = ''
                         ) -> PurePosixPath:
        return (
            self.get_ria_repo_path()
            / 'annex' / 'objects'
            / self.special_remote.annex.dirhash(key)
            / key / (key + extension)
        )

    def initremote(self):
        pass

    def prepare(self):
        pass

    def progress_handler(self, size: int, total_size: int):
        self.annex.progress(size)

    @abstractmethod
    def transfer_store(self, key: str, local_file: str):
        raise NotImplementedError

    @abstractmethod
    def transfer_retrieve(self, key: str, local_file: str):
        raise NotImplementedError

    @abstractmethod
    def remove(self, key: str):
        raise NotImplementedError

    @abstractmethod
    def checkpresent(self, key: str) -> bool:
        raise NotImplementedError
