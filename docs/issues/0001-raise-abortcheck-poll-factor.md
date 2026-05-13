---
title: Raise AbortCheck poll factor 20 → 200
type: AFK
status: done
---

## Parent

`IMPLEMENTATION_PLAN.md` — Stage 0.

## What to build

`AbortCheck` is called once per operator Pull in `src/query/plan/operator.cpp`.
It is already poll-amortized via a thread-local `ResettableCounter{20}` so the
real `MustAbort()` work runs only every 20th call — but the counter decrement +
branch on every Pull still costs ~2.2% of total samples on the 4-hop
edge-uniqueness benchmark.

Raise the counter from 20 to 200. Abort latency goes from ~20 Pulls to ~200
Pulls, which at hundreds of ns per Pull is still microseconds — invisible to
users and well within the existing timeout granularity.

Single change at `src/query/plan/operator.cpp:385`:

```cpp
thread_local auto maybe_check_abort = utils::ResettableCounter{200};
```

## Acceptance criteria

- [ ] `ResettableCounter{200}` in `operator.cpp:385`.
- [ ] `AbortCheck` self-time drops below 1% of total samples on the 4-hop
      `PROFILE` workload (verify via `tools/profile_edge_uniqueness.sh`).
- [ ] Existing `tests/e2e/transactions/` abort/timeout suites pass unchanged.
- [ ] No new tests added.

## Blocked by

None - can start immediately.
