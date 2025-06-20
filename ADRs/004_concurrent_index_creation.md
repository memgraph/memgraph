# Concurrent index creation ADR

**Author**
Gareth Lloyd (https://github.com/Ignition)

**Status**
ACCEPTED

**Date**
April 23, 2025

**Problem**

Currently, index creation is a unique access operation, meaning that:
- Queries can not run concurrently while the index is being made
- Allowing for a consistent index to be made
- One complete other queries can run again using the new index in the plan

**Criteria**

- Allow index population (most expensive part), to happen concurrent with other queries
- Allow read only queries to be uninterupted
- Planning can only use indexes once populated

**Decision**

We will leverage new storage access locking mode of READ_ONLY. This will allow readers to continue
uninterupted. Once the index is registered we downgrade the access to READ, allowing new writers.
The skip list based indexes are concurrently safe datastructures where propable entries are
inserted, MVCC is still required on scan to validated the index entry is correct to any given
transcation. The idea is that new writers can insert new entries, while the index creation is
populating for all entries consistent with its snapshot isolation. Once the index is populated,
it can be made availible for the planner to use.

Considerations:
- Replica, would also be able to use READ_ONLY downgrade to READ. But replica has no writters
  so this would be a perf win.
- TTL, enable + disable modified multiple internal indices. Can only downgrade once, need to
  take that into consideration.
- Auto indexing, ATM done during commit. Hence data and metadata update within same txn. This
  currently means that the replica changes the order and using UNIQUE access to make this work.
  We will need to initially keep replica as UNIQUE, but full fix is to do auto-indexing in
  separate txn.
