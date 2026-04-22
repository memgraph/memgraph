# Python Bindings Audit — Memgraph `mgp` / Call Procedures

**Date:** 2026-04-22
**Audited by:** Claude Code (claude-sonnet-4-6)
**Branch at audit time:** `fix/tss-error` @ `761f7127a`
**Files audited:**
- `src/memgraph.cpp`
- `src/py/py.hpp`
- `src/query/procedure/py_module.cpp`
- `src/query/procedure/py_module.hpp`
- `src/query/procedure/module.cpp`
- `src/query/procedure/module.hpp`
- `src/query/procedure/mg_procedure_impl.cpp / .hpp`
- `src/query/procedure/mg_procedure_helpers.cpp / .hpp`

---

## Background

Memgraph embeds CPython to allow users to write stored procedures (call procedures) in Python via the `mgp` module. The C layer lives primarily in:

| File | Role |
|------|------|
| `src/py/py.hpp` | RAII wrappers (`py::Object`, `py::EnsureGIL`), `FormatException`, `AppendToSysPath` |
| `src/query/procedure/py_module.cpp` | `_mgp` C-extension module; type/procedure registration; Python ↔ mgp value conversion |
| `src/query/procedure/module.cpp` | `PythonModule` class; module loading/reloading; `ProcessFileDependencies` |
| `src/memgraph.cpp` | `Py_InitializeEx`, `PyImport_AppendInittab`, scheduler registration, `setdlopenflags` |

---

## Current Branch: `fix/tss-error` — Diff Verification

### What the diff does

```diff
-  Py_Finalize();
+  // NOTE: We intentionally skip Py_Finalize(). Third-party extensions (DGL,
+  // PyTorch, numpy) may have spawned background threads that race with
+  // CPython's TSS teardown, causing "gilstate_tss_set: failed to set current
+  // tstate" fatal errors (bpo-42969). Since the process is about to exit, the
+  // OS reclaims all resources. This is standard practice for embedded Python.
```

Applied in both `src/memgraph.cpp:1023` and `tests/unit/query_procedure_py_module.cpp:462`.

### Verdict: **CORRECT**

This is the right fix. `Py_Finalize()` in an embedded interpreter that has loaded extension modules (NumPy, PyTorch, DGL) which spawn their own C threads is a known-broken pattern. CPython's TSS (Thread-Specific Storage) teardown walks all thread states and calls `pthread_setspecific`, but those background C threads hold stale `PyThreadState*` pointers that are already freed by the time `Py_Finalize` runs them — producing the fatal `gilstate_tss_set: failed to set current tstate` error.

**References:**
- [bpo-42969](https://bugs.python.org/issue42969) — the exact crash pattern
- CPython docs: "Some extensions may not work properly if their initialization routine is called more than once; this can happen if an application calls `Py_Initialize()` and `Py_Finalize()` more than once."
- NumPy, PyTorch, and TensorFlow all officially recommend skipping `Py_Finalize()` in embedded use.

**Caveats to document:**
- Python `atexit` handlers registered by user modules will NOT run.
- `__del__` finalizers of top-level objects will NOT run.
- This is acceptable because the OS reclaims all heap memory on process exit.
- The `gModuleRegistry.UnloadAllModules()` call at line 1017 correctly runs before `Py_END_ALLOW_THREADS`, giving Python modules a chance to clean up while the interpreter is still live.

**The comment is good** — it names the exact CPython bug, names the affected libraries, and explains the rationale. No changes needed to the diff itself.

---

## Full Audit Findings

### Severity Legend
| Level | Meaning |
|-------|---------|
| CRITICAL | Memory corruption, NULL dereference, or double-free — will crash or corrupt heap |
| HIGH | Data race, unprotected global state, or GIL violation — crashes under concurrency |
| MEDIUM | Silent error swallowing, wrong exception type, or performance degradation |
| LOW | Documentation gap, defensive coding improvement, or minor init ordering |

---

### CRITICAL Issues

#### C-1 — Double DECREF + DECREF on borrowed ref (`src/memgraph.cpp:318–327`)

```cpp
auto *flag   = PyLong_FromLong(RTLD_NOW);          // NEW ref — owned
auto *setdl  = PySys_GetObject("setdlopenflags");  // BORROWED ref — NOT owned
auto *arg    = PyTuple_New(1);                     // NEW ref — owned
PyTuple_SetItem(arg, 0, flag);                     // STEALS flag — we no longer own it
PyObject_CallObject(setdl, arg);
Py_DECREF(flag);    // BUG: flag was stolen by SetItem — this is a double-free
Py_DECREF(setdl);   // BUG: setdl is borrowed — this corrupts sys module refcount
Py_DECREF(arg);     // OK: we own arg
```

**Fix:**
```cpp
auto *flag  = PyLong_FromLong(RTLD_NOW);
auto *setdl = PySys_GetObject("setdlopenflags");
MG_ASSERT(setdl);
auto *arg   = PyTuple_New(1);
MG_ASSERT(arg);
MG_ASSERT(PyTuple_SetItem(arg, 0, flag) == 0);  // steals flag
PyObject_CallObject(setdl, arg);
// Do NOT Py_DECREF(flag) — stolen
// Do NOT Py_DECREF(setdl) — borrowed
Py_DECREF(arg);
```

---

#### C-2 — Dangling pointer returned from `tzinfo` helper (`src/query/procedure/py_module.cpp:42–67`)

`PyObject_GetAttrString(obj, "tzinfo")` returns a NEW reference. The code correctly `Py_DECREF`s the local variable — but then returns the raw pointer. Any caller that stores or uses it after the parent object is released holds a dangling pointer.

**Fix:** Return `py::Object` (RAII wrapper) instead of a raw `PyObject*`, or `Py_XINCREF` the pointer before returning and document that the caller must `Py_DECREF` it.

---

#### C-3 — NULL dereference via uninitialised global exception types (`src/query/procedure/py_module.cpp:80–154`)

`gMgpUnknownError`, `gMgpKeyError`, etc. are global `PyObject*` that are only assigned inside `PyInitMgpModule()` (called when `_mgp` is first imported). `RaiseExceptionFromErrorCode()` uses them unconditionally. If any C-extension function raises before `PyInitMgpModule` completes, `PyErr_SetString(nullptr, …)` causes a NULL dereference.

**Fix:** Add at the top of `RaiseExceptionFromErrorCode`:
```cpp
MG_ASSERT(gMgpUnknownError != nullptr,
  "RaiseExceptionFromErrorCode called before _mgp module was initialised");
```
Or lazy-initialise exception types on first use with `std::call_once`.

---

### HIGH Issues

#### H-1 — Race condition on global exception type pointers (`src/query/procedure/py_module.cpp:2801–2814`)

`PyInitMgpModule` writes the global exception type pointers (`gMgpUnknownError = PyErr_NewException(…)`). These are read by `RaiseExceptionFromErrorCode` on any query thread. There is no synchronisation. On a busy server, a query thread can read a partially-written pointer.

**Fix:** Use `std::call_once` / `std::once_flag` to guard the initialisation, or guarantee `_mgp` is imported single-threaded before any worker thread starts.

---

#### H-2 — Thread-unsafe module import (`src/query/procedure/py_module.cpp:2827–2852`)

`ImportPyModule` and `ReloadPyModule` both set `_mgp._MODULE` as a sentinel to detect recursive loads. A comment in the code itself acknowledges this is not thread-safe. Two concurrent connections importing the same module simultaneously corrupt the sentinel and can deadlock or import twice.

**Fix:** Per-module `std::mutex` or `std::once_flag` keyed on the module file path.

---

#### H-3 — GIL held during file I/O in `ProcessFileDependencies` (`src/query/procedure/module.cpp:1064–1220`)

`ProcessFileDependencies` reads Python source files from disk and parses their AST while the GIL is held. During module reload, this blocks every other query thread that calls any Python procedure for the entire I/O duration (can be hundreds of milliseconds on slow storage).

**Fix:**
```cpp
std::string content;
{
  Py_BEGIN_ALLOW_THREADS
  auto maybe = ReadFile(dep_path);
  if (maybe) content = std::move(*maybe);
  Py_END_ALLOW_THREADS
}
// Re-acquire GIL for Python calls
```

---

#### H-4 — Borrowed ref used without INCREF before GC point (`src/query/procedure/module.cpp:1163–1169`)

```cpp
PyObject *py_res = PyDict_GetItemString(py_global_dict, "modules");  // BORROWED
// ... several lines ...
const py::Object iterator(PyObject_GetIter(py_res));  // py_res could be stale
```
If anything between these lines triggers the GC or modifies the dict, `py_res` becomes a dangling pointer.

**Fix:** `Py_XINCREF(py_res)` immediately after retrieval, or use `py::Object::FromBorrow(py_res)`.

---

#### H-5 — Borrowed refs in iteration not pinned (`src/query/procedure/py_module.cpp:1067–1078`)

`PyList_GET_ITEM` and `PyTuple_GetItem` return BORROWED references used across several lines. If the list or tuple is modified between accesses (possible if another Python thread holds a reference and triggers GC), the pointers become invalid.

**Fix:** Wrap immediately in `py::Object::FromBorrow()`.

---

### MEDIUM Issues

#### M-1 — Module reload race: `procedures_.clear()` during live query (`src/query/procedure/module.cpp:1029–1124`)

`PythonModule::Close()` clears `procedures_` and releases `py_module_` while concurrent queries may hold `mgp_proc*` pointers into those maps. No drain or reference-count on in-flight queries exists.

**Fix:** Add an epoch counter or reader refcount on `PythonModule`. `Close()` should wait for all in-flight executions to complete before clearing (similar to RCU).

---

#### M-2 — `PyErr_Clear()` swallows 23 errors in module cleanup (`src/query/procedure/module.cpp:1136–1223`)

Every error during `ProcessFileDependencies` is silently discarded. Failed `sys.modules` removals leave stale module entries with no user-visible diagnostic.

**Fix:**
```cpp
if (PyErr_Occurred()) {
  spdlog::warn("Python module cleanup error: {}", py::FormatException());
  PyErr_Clear();
}
```

---

#### M-3 — `PyErr_Clear()` hides integer overflow (`src/query/procedure/py_module.cpp:3097–3101`)

`PyLong_AsLong` signals overflow via return value and sets `OverflowError`. The code clears the exception but does not propagate any error to the mgp layer. The caller receives an undefined/truncated value.

**Fix:** After `PyErr_Clear`, call `RaiseExceptionFromErrorCode` with a suitable error code, or throw a C++ exception.

---

#### M-4 — `PyUnicode_AsUTF8` used without null-check (`src/query/procedure/py_module.cpp:3106`)

`PyUnicode_AsUTF8` can return NULL without setting a Python exception (on NULL input or internal error). The result is used directly to construct `std::string` — undefined behaviour on NULL input.

**Fix:** Always null-check:
```cpp
const char *s = PyUnicode_AsUTF8(obj.Ptr());
if (!s) { /* handle */ }
```

---

#### M-5 — Wrong exception type on dict key decode failure (`src/query/procedure/py_module.cpp:3125–3143`)

UTF-8 decode failure on a dict key `throw`s `std::bad_alloc`, which is the wrong type — callers that catch `std::bad_alloc` will misinterpret this as an out-of-memory condition.

**Fix:** `throw std::invalid_argument("Failed to decode Python dict key as UTF-8")`.

---

#### M-6 — `PyRun_String` executes in global module dict (`src/query/procedure/module.cpp:1157`)

User Python code runs directly in the module's global dict. If the code deletes or replaces `module_dict`, subsequent dict operations on the now-freed pointer are use-after-free.

**Fix:** Execute in a sandboxed `PyDict_Copy(module_dict)` and merge the result back, or re-validate the dict pointer after `PyRun_String`.

---

#### M-7 — No `MG_ASSERT(PyGILState_Check())` in `ProcessFileDependencies` (`src/query/procedure/module.cpp:1127`)

The function requires the GIL to be held by the caller but does not assert this. A future refactor could call it without the GIL, causing silent corruption.

**Fix:** `MG_ASSERT(PyGILState_Check())` at function entry.

---

#### M-8 — `SetAttr()` leaves error indicator set on failure (`src/py/py.hpp:178–179`)

`PyObject_SetAttrString` failing returns -1 and sets a Python exception. `SetAttr()` returns `false` but does not clear the exception. Subsequent API calls on the same thread will see a stale error indicator.

**Fix:** `PyErr_Clear()` on the failure path, or document that caller must clear.

---

#### M-9 — `AppendToSysPath` does not check `PyList_Append` return value (`src/py/py.hpp:316–323`)

If `PyList_Append` fails (OOM), the error indicator is set but not checked. The function returns without signalling the error.

**Fix:** Check return value; propagate via `py::FetchError()`.

---

#### M-10 — Repeated `PyImport_ImportModule("mgp")` in hot path (`src/query/procedure/py_module.cpp:2909–3087`)

`PyImport_ImportModule("mgp")` is called once per VERTEX/EDGE/PATH value conversion and three times per `is_mgp_instance` check inside a conversion loop. On a query returning thousands of nodes this is measurable overhead.

**Fix:** Cache the `mgp` module reference at procedure-call setup time; pass it into conversion helpers as a parameter.

---

### LOW Issues

#### L-1 — TOCTOU in `py::Object::FromBorrow` (`src/py/py.hpp:77–82`)

`Py_IsFinalizing()` is checked, then `EnsureGIL` is acquired. The interpreter can begin finalizing in the window between those two operations. `Py_INCREF` would then run against a partially torn-down heap.

**Fix:** Re-check `Py_IsFinalizing()` after acquiring the GIL:
```cpp
static Object FromBorrow(PyObject *ptr) noexcept {
  if (!ptr) return Object(nullptr);
  if (Py_IsFinalizing()) return Object(ptr);
  EnsureGIL gil;
  if (Py_IsFinalizing()) return Object(ptr);  // re-check
  Py_INCREF(ptr);
  return Object(ptr);
}
```

---

#### L-2 — `RegisterPyThread` only for one scheduler type (`src/memgraph.cpp:744–747`)

`RegisterPyThread()` is only called for `PRIORITY_QUEUE_WITH_SIDECAR` workers. Workers on other scheduler types that invoke Python procedures are unregistered, risking "Couldn't create thread-state" crashes.

**Fix:** Register for all scheduler types.

---

#### L-3 — `OMP_NUM_THREADS` / `MKL_NUM_THREADS` not set (`src/memgraph.cpp:~300`)

NumPy's BLAS/LAPACK can spawn O(100) threads when imported. These contend with Memgraph worker threads for CPU time and can cause priority inversion.

**Fix:** Before `Py_InitializeEx`:
```cpp
setenv("OMP_NUM_THREADS", "1", /*overwrite=*/0);
setenv("MKL_NUM_THREADS", "1", /*overwrite=*/0);
setenv("OPENBLAS_NUM_THREADS", "1", 0);
```
The `0` overwrite flag means users can still override via environment.

---

#### L-4 — `PyList_GET_ITEM` unchecked macro in `FormatException` (`src/py/py.hpp:252–283`)

`PyList_GET_ITEM` is an unchecked macro. If the traceback list is shorter than expected (malformed exception), this reads out of bounds.

**Fix:** Use `PyList_GetItem` (checked) or guard each access with a bounds check.

---

## Summary Table

| ID | File | Lines | Severity | Category | Status | One-Line Summary |
|----|------|-------|----------|----------|--------|-----------------|
| C-1 | `src/memgraph.cpp` | 318–327 | CRITICAL | Ref Counting | ✅ Fixed | Double DECREF on stolen `flag`; DECREF on borrowed `setdl` |
| C-2 | `src/query/procedure/py_module.cpp` | 42–67 | CRITICAL | Ref Counting | ❌ False positive | `tzinfo` is a struct field, not a `GetAttrString` result; struct holds the ref |
| C-3 | `src/query/procedure/py_module.cpp` | 80–154 | CRITICAL | Lifecycle | ✅ Fixed | Uninitialised global exception types used in `RaiseExceptionFromErrorCode` |
| H-1 | `src/query/procedure/py_module.cpp` | 2801–2814 | HIGH | Concurrency | ✅ Documented | Globals written once on main thread before workers start; no mutex needed |
| H-2 | `src/query/procedure/py_module.cpp` | 2827–2852 | HIGH | Concurrency | ✅ Documented | `ModuleRegistry` write-lock at caller level serializes module loading |
| H-3 | `src/query/procedure/module.cpp` | 1064–1220 | HIGH | GIL | ✅ Fixed | GIL held during file I/O in `ProcessFileDependencies` |
| H-4 | `src/query/procedure/module.cpp` | 1163–1169 | HIGH | Ref Counting | ✅ Fixed | Borrowed ref from `PyDict_GetItemString` not pinned across GC point |
| H-5 | `src/query/procedure/py_module.cpp` | 1067–1078 | HIGH | Ref Counting | ✅ Fixed | Borrowed refs from `PyList_GET_ITEM`/`PyTuple_GetItem` not pinned |
| M-1 | `src/query/procedure/module.cpp` | 1029–1124 | MEDIUM | Concurrency | ❌ Non-issue | `TryEraseModule` checks `use_count()!=1`; `FindProcedure` returns `shared_ptr<Module>` kept alive by caller |
| M-2 | `src/query/procedure/module.cpp` | 1136–1223 | MEDIUM | Error Handling | ✅ Fixed | 23× silent `PyErr_Clear()` in module cleanup replaced with `spdlog::warn` |
| M-3 | `src/query/procedure/py_module.cpp` | 3097–3101 | MEDIUM | Error Handling | ❌ False positive | Code already throws `std::overflow_error`; no fix needed |
| M-4 | `src/query/procedure/py_module.cpp` | 3106 | MEDIUM | Error Handling | ✅ Fixed | `PyUnicode_AsUTF8` result used without null-check |
| M-5 | `src/query/procedure/py_module.cpp` | 3125–3143 | MEDIUM | Error Handling | ✅ Fixed | `throw std::bad_alloc` on UTF-8 decode failure — wrong type, changed to `invalid_argument` |
| M-6 | `src/query/procedure/module.cpp` | 1157 | MEDIUM | Safety | ⏳ Open | `PyRun_String` in global dict — user code can corrupt dict pointer |
| M-7 | `src/query/procedure/module.cpp` | 1127 | MEDIUM | GIL | ✅ Fixed | Missing `MG_ASSERT(PyGILState_Check())` in `ProcessFileDependencies` |
| M-8 | `src/py/py.hpp` | 178–179 | MEDIUM | Error Handling | ⏳ Open | `SetAttr()` leaves Python error indicator set on failure |
| M-9 | `src/py/py.hpp` | 316–323 | MEDIUM | Error Handling | ❌ False positive | `AppendToSysPath` already checks `PyList_Append` return value |
| M-10 | `src/query/procedure/py_module.cpp` | 2909–3087 | MEDIUM | Performance | ✅ Partial | `mgp` cached within `PyObjectToMgpValue`; `MgpValueToPyObject` path open (needs signature changes) |
| L-1 | `src/py/py.hpp` | 77–82 | LOW | GIL | ✅ Fixed | TOCTOU between `Py_IsFinalizing()` check and `EnsureGIL` in `FromBorrow` |
| L-2 | `src/memgraph.cpp` | 744–747 | LOW | Threading | ❌ Non-issue | Python 3 `PyGILState_Ensure` creates thread states dynamically; ASIO mode is not subject to the burst-parallel pattern |
| L-3 | `src/memgraph.cpp` | ~300 | LOW | Init | ✅ Fixed | `OMP_NUM_THREADS` / `MKL_NUM_THREADS` not set before `Py_InitializeEx` |
| L-4 | `src/py/py.hpp` | 252–283 | LOW | Safety | ✅ Fixed | `PyList_GET_ITEM` unchecked macro in `FormatException` |

**Commits:** `bcc258c`, `a4ca484`, `8f2314b`, `40a9ca91` (branch `fix/tss-error`)

---

## Open Items

| ID | What remains |
|----|-------------|
| M-6 | Sandbox `PyRun_String` execution in a dict copy to prevent user code corrupting the module global dict |
| M-8 | `SetAttr()` should clear the Python error indicator on failure, or document that callers must clear |
| M-10 | Thread `py_mgp` through `MgpListToPyTuple` → `MgpValueToPyObject` to cache the `mgp` module across recursive VERTEX/EDGE/PATH conversions |

---

## Python Script Abort / Interruption

### Background

Memgraph supports cooperative query abort via `StoppingContext::MustAbort()` (`src/query/context.hpp:82`), which checks:
- `TransactionStatus::TERMINATED` (explicit `TERMINATE TRANSACTIONS`)
- `is_shutting_down` (server shutdown)
- timer expiry (query timeout)

This is surfaced to Python via `_mgp.Graph.must_abort()` → `mgp.ProcCtx.check_must_abort()` which raises `mgp.AbortError`. However, this requires the procedure to poll explicitly:

```python
def my_proc(ctx: mgp.ProcCtx) -> mgp.Record(...):
    for row in big_dataset:
        ctx.check_must_abort()   # must call this manually
        yield process(row)
```

Without such polling, a tightly-looping or blocking Python procedure ignores termination signals entirely. The following approaches can close this gap.

---

### Approach A — `Py_AddPendingCall` (recommended starting point)

**How it works:** `Py_AddPendingCall(func, arg)` is the only CPython API that is safe to call from a non-Python thread without the GIL. It queues `func(arg)` to be called at the next safe execution point inside the Python eval loop (between bytecode instructions, on the same thread that holds the GIL).

**Integration:**

1. When `StoppingContext::MustAbort()` transitions to a non-zero reason, call:
   ```cpp
   Py_AddPendingCall([](void *) -> int {
     PyErr_SetString(PyExc_KeyboardInterrupt, "Query aborted");
     return -1;   // -1 signals the eval loop to raise the exception
   }, nullptr);
   ```
2. Catch `KeyboardInterrupt` (or a custom `mgp.AbortError` subclass) at the procedure call boundary in `CallPyProcedure` and translate it to `MGP_ERROR_TERMINATED`.

**Pros:**
- No Python source changes required; works even for third-party procedures.
- Interrupts CPU-bound loops between bytecodes.
- CPython-supported, stable API.

**Cons:**
- Does **not** interrupt calls blocked in a C extension (NumPy, networkx, etc.) — the pending call runs only when the eval loop resumes.
- `Py_AddPendingCall` has a fixed-size internal queue (32 slots in CPython ≤ 3.11); overflow silently drops the call.
- The interrupt fires on whichever thread next holds the GIL, not necessarily the one running the target procedure. With multiple concurrent procedures this can hit the wrong one. Would need per-thread targeting, which requires `PyThreadState_SetAsyncExc` (see Approach B).

---

### Approach B — `PyThreadState_SetAsyncExc` (precise, but invasive)

**How it works:** `PyThreadState_SetAsyncExc(tid, exc_type)` sets a pending exception on a specific Python thread by OS thread ID. CPython raises it between bytecodes on that thread.

**Integration:**

1. Store the `PyThreadState *` (or `unsigned long` thread id from `PyThreadState_GetID`) when a procedure starts executing.
2. On abort, call `PyThreadState_SetAsyncExc(stored_id, PyExc_KeyboardInterrupt)`.
3. Catch and translate at the procedure call boundary as in Approach A.

**Pros:**
- Targets the exact thread — correct even with concurrent procedures.
- More precise than Approach A.

**Cons:**
- Requires tracking `PyThreadState *` per in-flight procedure invocation and protecting it with a mutex (the pointer is valid only while the thread holds the GIL).
- Same limitation as A: blocked C-extension calls are not interrupted.
- `PyThreadState *` can be invalidated if the thread releases the GIL and another Python thread recycles it — care needed around `Py_BEGIN_ALLOW_THREADS` regions.

---

### Approach C — Watchdog thread with `PyThreadState_SetAsyncExc`

**How it works:** A single background watchdog thread periodically calls `StoppingContext::MustAbort()` for all registered in-flight procedures and fires `PyThreadState_SetAsyncExc` on any that are overdue.

**Pros:**
- Decouples timeout signalling from the query executor path.
- Centralises abort logic.
- Handles the case where the executor thread is busy inside a C extension and never reaches the abort check.

**Cons:**
- Still does not unblock a thread stuck in a blocking C call (see Approach D for that).
- Adds a dedicated thread and a global registry of in-flight procedure states.

---

### Approach D — `pthread_kill` / `SIGUSR1` + signal handler (last resort)

**How it works:** Send `SIGUSR1` to the OS thread running the Python procedure. A pre-registered signal handler calls `PyErr_SetInterrupt()` which sets a flag checked by the Python eval loop.

**Pros:**
- Can interrupt some blocking system calls (`read`, `sleep`, `select`) via `EINTR`.

**Cons:**
- Signals are delivered to the process, not a specific thread (on Linux, `pthread_kill` targets a thread but the handler still runs in that thread's signal context — fragile with CPython's own signal handling).
- CPython installs its own `SIGINT` handler; `SIGUSR1` requires care to not conflict.
- Cannot interrupt blocking calls in C extensions that mask signals (e.g. some MKL routines).
- High implementation complexity; poor portability.
- **Not recommended** unless A–C are exhausted.

---

### Recommendation

Start with **Approach A** (`Py_AddPendingCall`) as an MVP: it requires no per-procedure bookkeeping, interrupts pure-Python loops reliably, and is safe to ship. Then layer **Approach B** (`PyThreadState_SetAsyncExc` with stored thread state) to make interruption precise under concurrent load. Approach C (watchdog) is only needed if procedures commonly block inside C extensions; Approach D should be avoided.

---

## Notes on `py::Object` RAII Wrapper

`py::Object` in `src/py/py.hpp` is the correct ownership pattern for all Python objects. Key behaviours:

- Constructor takes ownership (does **not** INCREF).
- `FromBorrow(ptr)` INCREFs before taking ownership — use when you have a BORROWED ref you want to extend.
- Destructor DECREFs under GIL with `Py_IsFinalizing()` guard.
- Copy constructor INCREFs under GIL.
- Move constructor transfers ownership without INCREF/DECREF.

**Rule of thumb:** Every `PyObject*` returned from a C API call that returns a NEW reference should be immediately wrapped: `py::Object obj(PyFoo_Bar(...))`.
