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
