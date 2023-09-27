from datalad_next.annexremotes import (
    RemoteError,
    super_main,
)
from datalad_next.annexremotes.uncurl import (
    UncurlRemote,
)


class Ora2Remote(UncurlRemote):
    """
    git-annex special remote for storing and obtaining files in and from RIA
    stores. It is a reimplementation of an earlier ORA remote (until 0.19.x a
    part of the DataLad core library).

    ORA stands for "(git-annex) optional remote access". A key purpose
    for this remote is to provide its functionality without requiring a
    git-annex installation on the remote side, while using data structures
    that remain compatible for direct use with git-annex.


    Configuration
    -------------

    The behavior of this special remote can be tuned via a number of
    configuration settings.

    `datalad.ora.legacy-mode=yes|[no]`
      If enabled, all special remote operations fall back onto the
      legacy ``ORA`` special remote implementation. This mode is
      only provided for backward-compatibility.
    """
    def initremote(self):
        # we cannot simply run UncurlRemote.prepare(), because it needs
        # `.remotename` and this is not yet available until the remote is
        # fully initialized
        #self.prepare()
        ria_url = self.annex.getconfig('url')
        if not ria_url:
            # Adopt git-annex's style of messaging
            raise RemoteError('Specify a RIA store URL with url=')
        if not ria_url.startswith('ria+'):
            # Adopt git-annex's style of messaging
            raise RemoteError('ria+<scheme>://... URL expected for url=')

        # here we could do all kinds of sanity and version checks.
        # however, in some sense `initremote` is not very special in
        # this regard. Most (or all) of these checks would also run
        # in prepare() on every startup. Performing the checks here
        # would mean that we need to be able to do them with a not
        # yet completely initialized remote (and therefore possibly
        # not seeing a relevant remote-specific git-config). Therefore
        # we are not doing any checks here (for now).


def main():
    """CLI entry point installed as ``git-annex-remote-ora2``"""
    super_main(
        cls=Ora2Remote,
        # for now we go with a fixed (and different from -core) name.
        # ultimately, we could switch the name based on sys.argv[0]
        # and even have multiple entrypoints that behave differently
        # by setting particular configuration, all conditional on the
        # name
        remote_name='ora2',
        description=\
        "transport file content to and from datasets hosted in RIA stores",
    )
