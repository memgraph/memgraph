# How to publish new versions
## Prerequisites
1. Installed poetry
```
pip install poetry
```
2. Set up [API tokens](https://pypi.org/help/#apitoken)
3. Be a collaborator on [pypi](https://pypi.org/project/mgp/)

## Making changes
1. Make changes to the package
2. Bump version in `pyproject.tml`
3. `poetry build`
4. `poetry publish`

## Why is this not automatized?

Because someone always has to manually bump up the version in `pyproject.toml`

## Why does `_mgp.py` exists?
BEcause we are mocking here all the types that are created by memgraph
in order to fix typing errors in `mgp.py`.
