from datalad.api import Dataset

from datalad.customremotes.ria_utils import (
    create_ds_in_store,
    create_store,
)
from datalad.distributed.ora_remote import (
    SSHRemoteIO,
)
from datalad.distributed.tests.ria_utils import (
    common_init_opts,
    populate_dataset,
)
from datalad.support.exceptions import (
    CommandError,
)
from datalad.tests.utils_pytest import (
    assert_in,
    assert_not_in,
    assert_raises,
    assert_repo_status,
    has_symlink_capability,
)
from datalad.utils import Path
from datalad_ria.tests.fixtures import ria_sshserver_setup

def _test_initremote_basic(url, io, store, ds_path, link):

    ds_path = Path(ds_path)
    store = Path(store)
    link = Path(link)
    ds = Dataset(ds_path).create()
    populate_dataset(ds)

    init_opts = common_init_opts + ['url={}'.format(url)]

    # fails on non-existing storage location
    assert_raises(CommandError,
                  ds.repo.init_remote, 'ria-remote', options=init_opts)
    # Doesn't actually create a remote if it fails
    assert_not_in('ria-remote',
                  [cfg['name']
                   for uuid, cfg in ds.repo.get_special_remotes().items()]
                  )

    # fails on non-RIA URL
    assert_raises(CommandError, ds.repo.init_remote, 'ria-remote',
                  options=common_init_opts + ['url={}'.format(store.as_uri())]
                  )
    # Doesn't actually create a remote if it fails
    assert_not_in('ria-remote',
                  [cfg['name']
                   for uuid, cfg in ds.repo.get_special_remotes().items()]
                  )

    # set up store:
    create_store(io, store, '1')
    # still fails, since ds isn't setup in the store
    assert_raises(CommandError,
                  ds.repo.init_remote, 'ria-remote', options=init_opts)
    # Doesn't actually create a remote if it fails
    assert_not_in('ria-remote',
                  [cfg['name']
                   for uuid, cfg in ds.repo.get_special_remotes().items()]
                  )
    # set up the dataset as well
    create_ds_in_store(io, store, ds.id, '2', '1')
    # now should work
    ds.repo.init_remote('ria-remote', options=init_opts)
    assert_in('ria-remote',
              [cfg['name']
               for uuid, cfg in ds.repo.get_special_remotes().items()]
              )
    assert_repo_status(ds.path)
    # git-annex:remote.log should have:
    #   - url
    #   - common_init_opts
    #   - archive_id (which equals ds id)
    remote_log = ds.repo.call_git(['cat-file', 'blob', 'git-annex:remote.log'],
                                  read_only=True)
    assert_in("url={}".format(url), remote_log)
    [assert_in(c, remote_log) for c in common_init_opts]
    assert_in("archive-id={}".format(ds.id), remote_log)

    # re-configure with invalid URL should fail:
    assert_raises(
        CommandError,
        ds.repo.call_annex,
        ['enableremote', 'ria-remote'] + common_init_opts + [
            'url=ria+file:///non-existing'])
    # but re-configure with valid URL should work
    if has_symlink_capability():
        link.symlink_to(store)
        new_url = 'ria+{}'.format(link.as_uri())
        ds.repo.call_annex(
            ['enableremote', 'ria-remote'] + common_init_opts + [
                'url={}'.format(new_url)])
        # git-annex:remote.log should have:
        #   - url
        #   - common_init_opts
        #   - archive_id (which equals ds id)
        remote_log = ds.repo.call_git(['cat-file', 'blob',
                                       'git-annex:remote.log'],
                                      read_only=True)
        assert_in("url={}".format(new_url), remote_log)
        [assert_in(c, remote_log) for c in common_init_opts]
        assert_in("archive-id={}".format(ds.id), remote_log)

    # we can deal with --sameas, which leads to a special remote not having a
    # 'name' property, but only a 'sameas-name'. See gh-4259
    try:
        ds.repo.init_remote('ora2',
                            options=init_opts + ['--sameas', 'ria-remote'])
    except CommandError as e:
        if 'Invalid option `--sameas' in e.stderr:
            # annex too old - doesn't know --sameas
            pass
        else:
            raise
    # TODO: - check output of failures to verify it's failing the right way
    #       - might require to run initremote directly to get the output


def test_initremote_basic_sshurl(ria_sshserver, ria_sshserver_setup, tmp_path):
    """Test via SSH"""
    # retrieve all values from the ssh-server fixture
    ria_baseurl = ria_sshserver[0]
    # create all parameters _test_initremote_basic() requires
    io = SSHRemoteIO(ria_sshserver_setup['HOST'])
    ds_path = Path(tmp_path / 'my-ds')
    link = tmp_path / "link"
    # the store should be on the ssh server
    storepath = ria_sshserver[1]
    _test_initremote_basic(
        ria_baseurl, io, storepath, ds_path, link)


