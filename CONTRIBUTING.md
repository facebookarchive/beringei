# Contributing to Beringei
We want to make contributing to Beringei as easy and transparent as
possible.

## Our Development Process
We primarily develop Beringei on an internal branch at Facebook, with commits
continuously synced to GitHub. Pull requests are rebased onto this internal
branch and then synced back out.

## Pull Requests
We actively welcome your pull requests.

1. Fork the repo and create your branch from `master`.
2. Make sure all changes have appropriate tests.
3. If you've changed APIs, update the documentation.
4. Ensure `make test` passes.
5. clang-format the repo by running `beringei/clang-format.sh`
6. If you haven't already, complete the Contributor License Agreement ("CLA").

## Contributor License Agreement ("CLA")
In order to accept your pull request, we need you to submit a CLA. You only need
to do this once to work on any of Facebook's open source projects.

Complete your CLA here: <https://code.facebook.com/cla>

## Issues
We use GitHub issues to track public bugs. Please ensure your description is
clear and has sufficient instructions to be able to reproduce the issue.

Facebook has a [bounty program](https://www.facebook.com/whitehat/) for the safe
disclosure of security bugs. In those cases, please go through the process
outlined on that page and do not file a public issue.

## Coding Style
Coding style is handled entirely by
[clang-format](http://clang.llvm.org/docs/ClangFormat.html). Please make sure
to run it against all code changes.

You will need clang-format >=3.9.

The provided script `clang-format.sh` is an easy way to reformat the entire
repository.

## License
By contributing to Beringei, you agree that your contributions will be licensed
under the LICENSE file in the root directory of this source tree.
