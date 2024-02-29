from __future__ import annotations

import logging
from abc import (
    ABCMeta,
    abstractmethod,
)
from pathlib import PurePosixPath

from annexremote import Master


lgr = logging.getLogger('datalad.ria.riahandler')


class RIAHandler(metaclass=ABCMeta):
    def __init__(self,
                 annex: Master,
                 dataset_id: str,
                 ) -> None:
        self.annex = annex
        self.dataset_id = dataset_id

    def get_ria_path(self, key: str) -> PurePosixPath:
        # NOTE: this is not the final version
        return PurePosixPath(self.dataset_id) / key

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
