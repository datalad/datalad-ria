import logging
from datalad_next.annexremotes import (
    RemoteError,
    SpecialRemote,
    super_main
)

lgr = logging.getLogger('datalad.customremotes.ora_remote')


class RemoteCommandFailedError(Exception):
    pass


class RIARemoteError(RemoteError):
    pass


class OraRemote(SpecialRemote):
    """
    git-annex special remote 'ORA' for storing and obtaining files in and from
    RIA stores.

    It is a reimplementation of an earlier ORA remote (until 0.19.x a part of
    the DataLad core library).


    Configuration
    -------------

    The behavior of this special remote can be tuned via a number of
    configuration settings.

    `datalad.ora.legacy-mode=yes|[no]`
      If enabled, all special remote operations fall back onto the
      legacy ``ORA`` special remote implementation. This mode is
      only provided for backward-compatibility.
    """
    def __init__(self, annex):
        super().__init__(annex)
        # the following members will be initialized on prepare()
        # as they require access to the underlying repository
        self._repo = None
        # name of the special remote
        self._remotename = None
        # name of the corresponding Git remote
        self._gitremotename = None
        self.archive_id = None
        self._legacy_special_remote = None
        self.remote_dataset_tree_version = None
        self.remote_object_tree_version = None

    def initremote(self):
        if not self.archive_id:
            # The config manager can handle bare repos since datalad#6332
            self.archive_id = self._repo.config.get('datalad.dataset.id')
        pass

    def prepare(self):
        # determine if we are in legacy mode, and if so, fall back to legacy ORA
        if self.get_remote_gitcfg(
                'ora', 'legacy-mode', default='no').lower() == 'yes':
            # ATTENTION DEBUGGERS!
            # If we get here, we will bypass all the ora implementation!
            # Check __getattribute__() -- pretty much no other code in this
            # file will run! __getattribute__ will relay all top-level
            # operations to an instance of the legacy implementation
            from datalad_ria.legacy import LegacyORARemote
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