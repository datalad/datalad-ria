from datalad_next.annexremotes import (
    RemoteError,
    SpecialRemote,
    super_main
)


class OraRemote(SpecialRemote):
    """
    git-annex special remote 'ORA' for storing and obtaining files in and from
    RIA stores.

    It is a reimplementation of an earlier ORA remote (until 0.19.x a part of
    the DataLad core library).
    """
    def __init__(self, annex):
        super().__init__(annex)

    def initremote(self):
        pass

    def prepare(self):
        pass

    def transfer_store(self, key, filename):
        pass

    def transfer_retrieve(self, key, filename):
        pass

    def checkpresent(self, key):
        pass

    def remove(self, key):
        pass

    def getcost(self):
        pass

    def whereis(self, key):
        pass

    def checkurl(self, url):
        pass

    def claimurl(self, url):
        pass

    def getavailability(self):
        pass

    def getinfo(self):
        pass