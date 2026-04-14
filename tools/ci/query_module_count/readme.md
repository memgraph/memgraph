# Query Module Count Guide

This directory contains the scanner used to enumerate query module procedures and functions across Memgraph and MAGE.

## Usage

Run from the repository root:

```bash
python3 tools/ci/query_module_count/scan_query_modules.py --target all
python3 tools/ci/query_module_count/scan_query_modules.py --target memgraph
python3 tools/ci/query_module_count/scan_query_modules.py --target mage
```

Flags:

- `--target all`: scan both Memgraph and MAGE
- `--target memgraph`: scan `src` and `query_modules`
- `--target mage`: scan `mage/*`
- `--compact`: emit compact JSON

The script prints JSON to stdout with:

- per-scope roots
- procedures by language
- functions by language
- counts

## What Counts

Count:

- read procedures
- write procedures
- batch procedures
- functions

Do not count:

- stream transformations such as `@mgp.transformation`
- helper callbacks that are never registered
- argument/result builder calls by themselves

Preferred rule:

- count registrations, not implementations

## Registration Signals

### Python

Primary signals:

- `@mgp.read_proc`
- `@mgp.write_proc`
- `mgp.add_batch_read_proc(...)`
- `mgp.add_batch_write_proc(...)`
- `@mgp.function`

Rule:

- count one item per decorator or batch registration call

### C

Primary signals:

- `mgp_module_add_read_procedure(...)`
- `mgp_module_add_write_procedure(...)`
- `mgp_module_add_batch_read_procedure(...)`
- `mgp_module_add_batch_write_procedure(...)`
- `mgp_module_add_function(...)`

Ignore:

- `mgp_proc_add_*`
- `mgp_func_add_*`

Rule:

- count one item per `mgp_module_add_*` call

### C++

Primary signals:

- `AddProcedure(...)`
- `AddBatchProcedure(...)`
- `AddFunction(...)`
- `mgp::module_add_read_procedure(...)`
- `mgp::module_add_write_procedure(...)`
- `mgp::module_add_function(...)`

Useful markers:

- `mgp::ProcedureType::Read`
- `mgp::ProcedureType::Write`

Ignore:

- `mgp::Parameter(...)`
- `mgp::Return(...)`

Rule:

- count one item per registration call

### Rust

Primary signals:

- `memgraph.add_read_procedure(...)`
- `memgraph.add_write_procedure(...)`
- `init_module!(...)`

Cross-check only:

- `define_procedure!(...)`

Rule:

- count the registration call, not the procedure macro body

## Notes

- Runtime module names do not always match source filenames; the scanner resolves many of them from CMake and install metadata.
- Built-in `mg.*` procedures are also included.
- The original declaration patterns were cross-checked against the Memgraph documentation for Python, C, C++, and Rust query modules.
