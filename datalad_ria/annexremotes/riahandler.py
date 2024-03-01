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

from datalad_next.annexremotes import SpecialRemote


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

    def get_ria_path(self, key: str) -> PurePosixPath:
        # NOTE: this is not the final version
        return self.base_path / self.dataset_id / key

    def initremote(self) -> bool:
        return True

    def prepare(self) -> bool:
        return True

    @abstractmethod
    def transfer_store(self, key: str, local_file: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def transfer_retrieve(self, key: str, local_file: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def remove(self, key: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def checkpresent(self, key: str) -> bool:
        raise NotImplementedError
