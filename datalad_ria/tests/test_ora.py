from pathlib import Path
import pytest
import re

from datalad_next.runners import (
    CommandError,
)

# we may witch this to just 'ora', once we have feature parity
ora_external_type = 'ora2'


def test_ora_initremote_errors(existing_dataset):
    ds = existing_dataset
    repo = ds.repo
    store_path = Path.cwd()

    ir_cmd = [
        'initremote',
        f'test-{ora_external_type}',
        'type=external',
        f'externaltype={ora_external_type}',
        'encryption=none',
    ]

    with pytest.raises(
        CommandError,
        match='Specify a RIA store URL with url=',
    ):
        repo.call_annex(ir_cmd)
    with pytest.raises(
        CommandError,
        match=re.escape('ria+<scheme>://... URL expected for url='),
    ):
        # use a file:// url here
        repo.call_annex(ir_cmd + [f'url={store_path.as_uri()}'])


def test_ora_localops(ria_store_localaccess, populated_dataset):
    ds = populated_dataset
    repo = ds.repo
    _, store_path = ria_store_localaccess

    ir_cmd = [
        'initremote',
        f'test-{ora_external_type}',
        'type=external',
        f'externaltype={ora_external_type}',
        'encryption=none',
    ]

    # smoke test that it can run
    repo.call_annex(ir_cmd + [f'url=ria+{store_path.as_uri()}'])

    cp_cmd = [
        'copy',
        '-t', f'test-{ora_external_type}',
    ]

    repo.call_annex(cp_cmd + ['one.txt'])
    # the annex key properties (and dirhash) are determined by the
    # file content and the MD5E backend default.
    # If neither of those changes, they must not change
    key_fpath = store_path / \
        ds.id[:3] / ds.id[3:] / 'annex' / 'objects' / \
        'X9' / '6J' / \
        'MD5E-s8--7e55db001d319a94b0b713529a756623.txt' / \
        'MD5E-s8--7e55db001d319a94b0b713529a756623.txt'
    assert key_fpath.exists()
    assert key_fpath.read_text() == 'content1'

    rm_cmd = [
        'drop',
        '-f', f'test-{ora_external_type}',
    ]

    repo.call_annex(rm_cmd + ['one.txt'])
    assert not key_fpath.exists()
    # TODO the parent directory stays (for now)
    # https://github.com/datalad/datalad-next/issues/454
    assert key_fpath.parent.exists()
