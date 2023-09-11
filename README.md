# DataLad RIA tools

[![Build status](https://ci.appveyor.com/api/projects/status/g9von5wtpoidcecy/branch/main?svg=true)](https://ci.appveyor.com/project/mih/datalad-extension-template/branch/main) [![codecov.io](https://codecov.io/github/datalad/datalad-extension-template/coverage.svg?branch=main)](https://codecov.io/github/datalad/datalad-extension-template?branch=main) [![crippled-filesystems](https://github.com/datalad/datalad-extension-template/workflows/crippled-filesystems/badge.svg)](https://github.com/datalad/datalad-extension-template/actions?query=workflow%3Acrippled-filesystems) [![docs](https://github.com/datalad/datalad-extension-template/workflows/docs/badge.svg)](https://github.com/datalad/datalad-extension-template/actions?query=workflow%3Adocs)

This datalad extension will contain a modernized and improved implementation for RIA-related functionality.
It is currently a work-in-progress.

TODOs:

- [x] Pick a name for the new extension.
- [x] Look through the sources and replace `helloworld` with
  `<newname>` (hint: `git grep helloworld` should find all
  spots).
- [ ] Delete the example command implementation in `datalad_helloworld/hello_cmd.py`.
- [ ] Implement a new command, and adjust the `command_suite` in
  `datalad_helloworld/__init__.py` to point to it.
- [ ] Replace `hello_cmd` with the name of the new command in
  `datalad_helloworld/tests/test_register.py` to automatically test whether the
  new extension installs correctly.
- [ ] Adjust the documentation in `docs/source/index.rst`. Refer to [`docs/README.md`](docs/README.md) for more information on documentation building, testing and publishing.
- [ ] Replace this README, and/or update the links in the badges at the top.
- [x] Update `setup.cfg` with appropriate metadata on the new extension.
- [ ] Generate GitHub labels for use by the "Add changelog.d snippet" and
  "Auto-release on PR merge" workflows by using the code in the
  `datalad/release-action` repository [as described in its
  README](https://github.com/datalad/release-action#command-labels).

- [ ] You can consider filling in the provided [.zenodo.json](.zenodo.json) file with
contributor information and [meta data](https://developers.zenodo.org/#representation)
to acknowledge contributors and describe the publication record that is created when
[you make your code citeable](https://guides.github.com/activities/citable-code/)
by archiving it using [zenodo.org](https://zenodo.org/).
- [ ] You may also want to
consider acknowledging contributors with the
[allcontributors bot](https://allcontributors.org/docs/en/bot/overview).

# Test setup

The test battery included in the sources is designed to run tests against a
configurable server. The default mode of operation is to perform all tests against
localhost (given the necessary service availability). A number of environment
variable (or configuration items) can be set to configure an alternative test target.

- `DATALAD_TESTS_RIA_SERVER_SSH_HOST`: Hostname of the SSH server
- `DATALAD_TESTS_RIA_SERVER_SSH_PORT`: Port of the SSH server
- `DATALAD_TESTS_RIA_SERVER_SSH_LOGIN`: Username/login to use with the SSH server
- `DATALAD_TESTS_RIA_SERVER_SSH_SECKEY`: Path to a local SSH private key to use for logging into the SSH server
- `DATALAD_TESTS_RIA_SERVER_SSH_PATH`: Root path to run all tests under on the SSH server (absolute path)
- `DATALAD_TESTS_RIA_SERVER_LOCALPATH`: If the directory pointed to by `DATALAD_TESTS_RIA_SERVER_SSH_PATH` is also available as a local mount, this is the path to the reespective local directory.


# Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) if you are interested in internals or
contributing to the project.
