"""DataLad demo command"""

__docformat__ = 'restructuredtext'

from typing import (
    Dict,
)

from datalad_next.commands import (
    EnsureCommandParameterization,
    ValidatedInterface,
    Parameter,
    build_doc,
    datasetmethod,
    eval_results,
    get_status_dict,
)
from datalad.interface.common_opts import (
    recursion_flag,
    recursion_limit
)
from datalad_next.constraints import (
    EnsureBool,
    EnsureChoice,
    EnsureInt,
    EnsureURL,
    EnsureRange,
    # we can have this with next's #324
    #EnsureRemoteName,
    EnsureStr,
)
from datalad_next.constraints.dataset import EnsureDataset
from datalad_next.utils import ParamDictator


import logging
lgr = logging.getLogger('datalad.ria.create_sibling_ria2')


class CreateSiblingRIAValidator(EnsureCommandParameterization):
    def joint_validation(self, params: Dict, on_error: str) -> Dict:
        p = ParamDictator(params)
        if not params['name']:
            params['name'] = 'ria'

        if p.storage_sibling == 'off' and p.storage_name:
            lgr.warning(
                "Storage sibling setup disabled, but a storage sibling name "
                "was provided"
            )
        if p.storage_sibling != 'off' and not p.storage_name:
            p.storage_name = f"{p.name}-storage"

        if p.storage_sibling != 'off' and p.name == p.storage_name:
            # leads to unresolvable, circular dependency with publish-depends
            raise ValueError("sibling names must not be equal")

        return params


@build_doc
class CreateSiblingRia(ValidatedInterface):
    # first docstring line is used a short description in the cmdline help
    # the rest is put in the verbose help and manpage
    """Creates a sibling to a dataset in a RIA store

    Just a shim implementation for now
    """

    _params_ = dict(
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""specify the dataset to process.  If
            no dataset is given, an attempt is made to identify the dataset
            based on the current working directory""",
            ),
        url=Parameter(
            args=("url",),
            metavar="ria+<ssh|file|http(s)>://<host>[/path]",
            doc="""URL identifying the target RIA store and access protocol.""",
            ),
        name=Parameter(
            args=('-s', '--name',),
            metavar='NAME',
            doc="""Name of the sibling.
            With `recursive`, the same name will be used to label all
            the subdatasets' siblings.""",
            ),
        storage_name=Parameter(
            args=("--storage-name",),
            metavar="NAME",
            doc="""Name of the storage sibling (git-annex special remote).
            Must not be identical to the sibling name. If not specified,
            defaults to the sibling name plus '-storage' suffix. If only
            a storage sibling is created, this setting is ignored, and
            the primary sibling name is used.""",
            ),
        alias=Parameter(
            args=('--alias',),
            metavar='ALIAS',
            doc="""Alias for the dataset in the RIA store.
            Add the necessary symlink so that this dataset can be cloned from the RIA
            store using the given ALIAS instead of its ID.
            With `recursive=True`, only the top dataset will be aliased.""",
            ),
        post_update_hook=Parameter(
            args=("--post-update-hook",),
            doc="""Enable Git's default post-update-hook for the created
            sibling. This is useful when the sibling is made accessible via a
            "dumb server" that requires running 'git update-server-info'
            to let Git interact properly with it.""",
            action="store_true"),
        shared=Parameter(
            args=("--shared",),
            metavar='{false|true|umask|group|all|world|everybody|0xxx}',
            doc="""If given, configures the permissions in the
            RIA store for multi-users access.
            Possible values for this option are identical to those of
            `git init --shared` and are described in its documentation.""",
            ),
        group=Parameter(
            args=("--group",),
            metavar="GROUP",
            doc="""Filesystem group for the repository. Specifying the group is
            crucial when [CMD: --shared=group CMD][PY: shared="group" PY]""",
            ),
        storage_sibling=Parameter(
            args=("--storage-sibling",),
            dest='storage_sibling',
            metavar='MODE',
            doc="""By default, an ORA storage sibling and a Git repository
            sibling are created ([CMD: on CMD][PY: True|'on' PY]).
            Alternatively, creation of the storage sibling can be disabled
            ([CMD: off CMD][PY: False|'off' PY]), or a storage sibling
            created only and no Git sibling
            ([CMD: only CMD][PY: 'only' PY]). In the latter mode, no Git
            installation is required on the target host."""),
        existing=Parameter(
            args=("--existing",),
            constraints=EnsureChoice('skip', 'error', 'reconfigure'),
            metavar='MODE',
            doc="""Action to perform, if a (storage) sibling is already
            configured under the given name and/or a target already exists.
            In this case, a dataset can be skipped ('skip'), an existing target
            repository be forcefully re-initialized, and the sibling
            (re-)configured ('reconfigure'), or the command be instructed to
            fail ('error').""", ),
        new_store_ok=Parameter(
            args=("--new-store-ok",),
            action='store_true',
            doc="""When set, a new store will be created, if necessary. Otherwise, a sibling
            will only be created if the url points to an existing RIA store.""",
        ),
        recursive=recursion_flag,
        recursion_limit=recursion_limit,
        trust_level=Parameter(
            args=("--trust-level",),
            metavar="TRUST-LEVEL",
            doc="""specify a trust level for the storage sibling. If not
            specified, the default git-annex trust level is used. 'trust'
            should be used with care (see the git-annex-trust man page).""",
        ),
    )

    shared_choices = EnsureChoice(
            'false', 'true', 'umask', 'group', 'all', 'world', 'everybody'
    )

    _validators = dict(
        url=EnsureURL(
            match='^ria\+(http|https|file|ssh)://',
        ),
        dataset=EnsureDataset(
            installed=True, purpose='create WebDAV sibling(s)'),
        # we can have this with next's #324
        #name=EnsureRemoteName(preexists=False),
        #storage_name=EnsureRemoteName(preexists=False),
        storage_sibling=EnsureChoice(
            'on', 'off', 'only'
        ),
        shared=shared_choices | EnsureStr(match='^[0-9]{4}$'),
        group=EnsureStr(),
        existing=EnsureChoice('skip', 'error', 'reconfigure'),
        recursive=EnsureBool(),
        recursion_limit=EnsureInt() & EnsureRange(min=0),
        trust_level=EnsureChoice('trust', 'semitrust', 'untrust'),
    )
    _validator_ = CreateSiblingRIAValidator(
        _validators,
        validate_defaults=('dataset',),
    )


    @staticmethod
    @datasetmethod(name='create_sibling_ria2')
    @eval_results
    def __call__(url,
                 name,
                 *,  # note that `name` is required but not posarg in CLI
                 dataset=None,
                 storage_name=None,
                 alias=None,
                 post_update_hook=False,
                 shared=None,
                 group=None,
                 storage_sibling=True,
                 existing='error',
                 new_store_ok=False,
                 trust_level=None,
                 recursive=False,
                 recursion_limit=None,
                 ):

        ds = dataset.ds
        yield get_status_dict(dataset=ds.pathobj,
                              action='create_sibling_ria',
                              status='not needed',
                              message='not yet implemented')
