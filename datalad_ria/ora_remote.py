import uuid

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

        # TODO run _get_ria_dsid() to confirm the validity of the ID
        # setup

        # here we could do all kinds of sanity and version checks.
        # however, in some sense `initremote` is not very special in
        # this regard. Most (or all) of these checks would also run
        # in prepare() on every startup. Performing the checks here
        # would mean that we need to be able to do them with a not
        # yet completely initialized remote (and therefore possibly
        # not seeing a relevant remote-specific git-config). Therefore
        # we are not doing any checks here (for now).

    def prepare(self):
        # UUID to use for this dataset in the store
        dsid = self._get_ria_dsid()

        # check for a remote-specific uncurl config
        # self.get_remote_gitcfg() would also consider a remote-type
        # general default, which is undesirable here
        tmpl_var = f'remote.{self.remotename}.uncurl-url'
        url_tmpl = self.repo.config.get(tmpl_var, None)
        if url_tmpl is None:
            # pull the recorded ria URL from git-annex
            ria_url = self.annex.getconfig('url')
            assert ria_url.startswith('ria+')
            # TODO check the layout settings of the actual store
            # to match this template
            base_url = ria_url[4:]
            url_tmpl = (
                # we fill in base url and dsid directly here (not via
                # uncurl templating), because it is simpler
                f'{base_url}/{dsid[:3]}/{dsid[3:]}/annex/objects/'
                # RIA v? uses the "mixed" dirhash
                '{annex_dirhash}{annex_key}/{annex_key}'
            )
        # we set the URL template in the config for the base class
        # routines to find
        self.repo.config.set(tmpl_var, url_tmpl, scope='override')
        # the rest is UNCURL "business as usual"
        super().prepare()

    def delete(self, key):
        # delete the key
        super().delete(key)
        # TODO depending on the nature of RIA store, we also should remove
        # the key directory, and possibly also the dirhash-type parents.
        # so possibly we need to make up to three additional deletion
        # requests via the handler. This can be rather slow, unless the
        # handler is clever (or we can instruct the handler to trigger
        # all deletions in one go

    #
    # helpers
    #
    def _get_ria_dsid(self):
        # check if the remote has a particular dataset ID configured
        # via git-annex
        dsid = self.annex.getconfig('archive-id')
        # if not, fall back on the datalad dataset ID
        if not dsid:
            dsid = self.repo.config.get('datalad.dataset.id')
        # under all circumstances this must be a valid UUID
        try:
            uuid.UUID(dsid)
        except ValueError as e:
            raise RemoteError(
                'No valid dataset UUID identifier found,'
                'specify via archive-id='
            ) from e
        return dsid


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
