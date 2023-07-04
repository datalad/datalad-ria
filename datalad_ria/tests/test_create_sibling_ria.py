from datalad_next.tests.fixtures import *
from datalad_next.utils import chpwd
from datalad_next.constraints import ConstraintError
from datalad.api import create_sibling_ria2


def test_parameter_validation(existing_dataset):
    wrong_urls = ['Not-a-url-at-all', 'http://missing/ria/prefix',
                  'ria+bogusprotocol://some/thing', 'ria+file://almost/correct']
    for url in wrong_urls:
        with pytest.raises(ConstraintError):
            create_sibling_ria2(url)
    correct_urls = ['ria+file:///tmp/fileurl', 'ria+http://httpurl',
                    'ria+ssh://myuser@someserver.university.org:/some/thing']
    for url in correct_urls:
        ds = existing_dataset
        with chpwd(ds.path):
            res = create_sibling_ria2(
                url,
                name='ria',
                dataset=ds,
            )



def test_warnings(caplog, existing_dataset):
    url = 'ria+file:///tmp/fileurl'
    ds = existing_dataset
    # check for warnings
    with caplog.at_level(logging.WARNING, logger='datalad.ria.create_sibling_ria2'):
        res = create_sibling_ria2(url,
                                  dataset=ds,
                                  name='ria',
                                  storage_sibling='off',
                                  storage_name='helloworld')
        text = "Storage sibling setup disabled, but a storage sibling name was " \
               "provided"
        assert text in caplog.text

    with pytest.raises(ValueError, match="sibling names must not be equal"):
        res = create_sibling_ria2(url,
                                  name='ria2',
                                  dataset=ds,
                                  storage_name='ria2'
                )




