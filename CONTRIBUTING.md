# How to contribute?

This is a general purpose guide for contributing to Memgraph. We're still
working out the kinks to make contributing to this project as easy and
transparent as possible, but we're not quite there yet. Hopefully, this document
makes the process for contributing clear and answers some questions that you may
have.

- [How to contribute?](#how-to-contribute)
  - [Open development](#open-development)
  - [Branch organization](#branch-organization)
  - [Bugs & changes](#bugs--changes)
    - [Where to find known issues?](#where-to-find-known-issues)
    - [Proposing a change](#proposing-a-change)
    - [Your first pull request](#your-first-pull-request)
    - [Sending a pull request](#sending-a-pull-request)
    - [Style guide](#style-guide)
  - [How to get in touch?](#how-to-get-in-touch)
  - [Code of Conduct](#code-of-conduct)
  - [License](#license)
  - [Attribution](#attribution)

## Open development

All work on Memgraph is done via [GitHub](https://github.com/memgraph/memgraph).
Both core team members and external contributors send pull requests which go
through the same review process.

## Branch organization

Most pull requests should target the [`master
branch`](https://github.com/memgraph/memgraph/tree/master). We only use separate
branches for developing new features and fixing bugs before they are merged with
`master`. We do our best to keep `master` in good shape, with all tests passing.

Code that lands in `master` must be compatible with the latest stable release.
It may contain additional features but no breaking changes if it's not
absolutely necessary. We should be able to release a new minor version from the
tip of `master` at any time.

## Bugs & changes

### Where to find known issues?

We are using [GitHub Issues](https://github.com/memgraph/memgraph/issues) for
our public bugs. We keep a close eye on this and try to make it clear when we
have an internal fix in progress. Before filing a new task, try to make sure
your problem doesn't already exist.

### Proposing a change

If you intend to change the public API, or make any non-trivial changes to the
implementation, we recommend [filing an
issue](https://github.com/memgraph/memgraph/issues/new). This lets us reach an
agreement on your proposal before you put significant effort into it.

If you're only fixing a bug, it's fine to submit a pull request right away but
we still recommend to file an issue detailing what you're fixing. This is
helpful in case we don't accept that specific fix but want to keep track of the
issue.

### Your first pull request

Working on your first Pull Request? You can learn how from this free video
series:

**[How to Contribute to an Open Source Project on
GitHub](https://app.egghead.io/courses/how-to-contribute-to-an-open-source-project-on-github)**

If you decide to fix an issue, please be sure to check the comment thread in
case somebody is already working on a fix. If nobody is working on it at the
moment, please leave a comment stating that you intend to work on it so other
people don't accidentally duplicate your effort.

If somebody claims an issue but doesn't follow up for more than two weeks, it's
fine to take it over but you should still leave a comment.

### Sending a pull request

The core team is monitoring for pull requests. We will review your pull request
and either merge it, request changes to it, or close it with an explanation.
**Before submitting a pull request,** please make sure the following is done:

1. Fork [the repository](https://github.com/memgraph/memgraph) and create your
   branch from `master`.
2. If you've fixed a bug or added code that should be tested, add tests!
3. Use the formatter `clang-format` for C/C++ code and `flake8` for Python code.
   `clang-format` will automatically detect the `.clang-format` file in the root
   directory while `flake8` can be used with the default configuration.

### Style guide

Memgraph uses the [Google Style
Guide](https://google.github.io/styleguide/cppguide.html) for C++ in most of its
code. You should follow them whenever writing new code.

## How to get in touch?

Aside from communicating directly via Pull Requests and Issues, the Memgraph
Community [Discord Server](https://discord.gg/memgraph) is the best place for
conversing with project maintainers and other community members.

## [Code of Conduct](https://github.com/memgraph/memgraph/blob/master/CODE_OF_CONDUCT.md)

Memgraph has adopted the [Contributor
Covenant](https://www.contributor-covenant.org/) as its Code of Conduct, and we
expect project participants to adhere to it. Please read [the full
text](https://github.com/memgraph/memgraph/blob/master/CODE_OF_CONDUCT.md) so
that you can understand what actions will and will not be tolerated.

## License

By contributing to Memgraph, you agree that your contributions will be licensed
under the [Memgraph licensing
scheme](https://github.com/memgraph/memgraph/blob/master/LICENSE).

## Attribution

This Contributing guide is adapted from the **React.js Contributing guide**
available at
[https://reactjs.org/docs/how-to-contribute.html](https://reactjs.org/docs/how-to-contribute.html).
