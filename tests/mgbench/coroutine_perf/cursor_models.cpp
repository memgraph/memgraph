// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Standalone microarchitectural study of cursor-pull dispatch designs (P3.3 coroutine cursors).
//
// Goal: isolate the PURE per-pull dispatch cost of each design, free of Memgraph's storage /
// jemalloc / profiling confounds, using the SAME coroutine machinery as the kernel (symmetric
// transfer, persistent gen_, the ResumeAwaitable/Awaiter protocol copied verbatim in spirit).
//
// Topology (models `MATCH (n)-[:R]->(m) RETURN count(*)`):
//   Source  : emits N rows                          (== ScanAll over N vertices)
//   Expand  : per source row, emits F rows          (== Expand with fan-out F; F=1 -> "chain")
//   driver  : pulls Expand to exhaustion, counts    (== Aggregate(count) driving its child)
// The value of each emitted row is fed through do_work(v, W) (W tunable) so we can dial per-row
// real work from ~0 (pull-dense, worst case) upward, reproducing "overhead ~ 1/per-row-work".
//
// Designs (argv[1]):
//   legacy   : virtual bool Pull() cursor pipeline                       (the flag-OFF baseline)
//   fused    : the whole pipeline is ONE coroutine (no per-cursor frames)(theoretical best coro)
//   allcoro  : every cursor a persistent-gen_ coroutine, symmetric xfer  (current kernel design)
//   mixed    : leaf Source on the frame-less immediate path, rest coro   (selective conversion)
//   helper   : allcoro + Expand co_awaits a ONE-SHOT helper coroutine    (the InitEdgesCo anti-pattern)
//             per input pull (fresh frame alloc/destroy per call)
//   allcoroY : allcoro + a per-inner-iteration co_await YieldPoint no-op  (models the yield check)
//
// Build+run: tests/mgbench/coroutine_perf/cursor_models_run.sh
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

// ───────────────────────── workload (identical across designs) ─────────────────────────
static unsigned long long g_sink = 0;

// noinline so the per-row work cost is byte-identical in every design (no design-specific inlining).
__attribute__((noinline)) static uint64_t do_work(uint64_t v, int W) {
  uint64_t a = v;
  for (int i = 0; i < W; ++i) a = a * 6364136223846793005ULL + 1442695040888963407ULL;
  return a;
}

static inline uint64_t mkval(uint64_t base, uint64_t f) { return base * 1000003ULL + f + 1; }

// ───────────────────────── coroutine core (mirror of cursor_awaitable_core.hpp) ─────────────────────────
struct BasePromise {
  bool has_more_{false};
  std::coroutine_handle<> parent_{std::noop_coroutine()};

  static constexpr std::suspend_always initial_suspend() noexcept { return {}; }

  struct SymmetricTransfer {
    std::coroutine_handle<> continuation;

    bool await_ready() const noexcept { return false; }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<>) const noexcept { return continuation; }

    void await_resume() const noexcept {}
  };

  auto final_suspend() noexcept { return SymmetricTransfer{parent_}; }

  void return_value(bool h) noexcept { has_more_ = h; }

  auto yield_value(bool h) noexcept {
    has_more_ = h;
    return SymmetricTransfer{parent_};
  }

  void unhandled_exception() noexcept { std::abort(); }
};

struct PullAwaitable {
  struct promise_type final : BasePromise {
    PullAwaitable get_return_object() noexcept {
      return PullAwaitable{std::coroutine_handle<promise_type>::from_promise(*this)};
    }
  };

  // one-shot helper awaiter (rvalue co_await -> owns + destroys the frame): models InitEdgesCo.
  struct Awaiter {
    std::coroutine_handle<promise_type> handle_{};
    bool owns_{false};

    ~Awaiter() {
      if (owns_ && handle_) handle_.destroy();
    }

    bool await_ready() const noexcept { return !handle_; }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> p) const noexcept {
      handle_.promise().parent_ = p;
      return handle_;
    }

    bool await_resume() const noexcept { return handle_.promise().has_more_; }
  };

  std::coroutine_handle<promise_type> handle_{};
  PullAwaitable() = default;

  explicit PullAwaitable(std::coroutine_handle<promise_type> h) noexcept : handle_(h) {}

  PullAwaitable(PullAwaitable &&o) noexcept : handle_(std::exchange(o.handle_, {})) {}

  PullAwaitable &operator=(PullAwaitable &&o) noexcept {
    if (this != &o) {
      if (handle_) handle_.destroy();
      handle_ = std::exchange(o.handle_, {});
    }
    return *this;
  }

  PullAwaitable(const PullAwaitable &) = delete;
  PullAwaitable &operator=(const PullAwaitable &) = delete;

  ~PullAwaitable() {
    if (handle_) handle_.destroy();
  }

  auto operator co_await() && noexcept { return Awaiter{std::exchange(handle_, {}), true}; }

  // persistent-generator resume (mirror of ResumeAwaitable: non-owning, immediate-mode for legacy children)
  struct ResumeAwaitable {
    std::coroutine_handle<promise_type> handle_{};
    bool imm_ready_{false};
    bool imm_val_{false};

    static ResumeAwaitable Immediate(bool v) noexcept { return ResumeAwaitable{{}, true, v}; }

    bool await_ready() const noexcept { return imm_ready_ || !handle_ || handle_.done(); }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> p) const noexcept {
      handle_.promise().parent_ = p;
      return handle_;
    }

    bool await_resume() const noexcept {
      if (imm_ready_) return imm_val_;
      if (!handle_ || handle_.done()) return false;
      return handle_.promise().has_more_;
    }

    bool Done() const noexcept { return imm_ready_ || !handle_ || handle_.done(); }

    bool Result() const noexcept { return imm_ready_ ? imm_val_ : (handle_ ? handle_.promise().has_more_ : false); }
  };

  ResumeAwaitable Resume() noexcept { return ResumeAwaitable{handle_}; }
};

enum class Status : uint8_t { Row, Done };

// root driver step (mirror of ResumePullStep, yield-off path)
static inline Status ResumePullStep(PullAwaitable::ResumeAwaitable &ra) {
  if (ra.Done()) return ra.Result() ? Status::Row : Status::Done;
  ra.handle_.resume();
  return ra.Result() ? Status::Row : Status::Done;
}

// a no-op yield-point co_await (mirror of YieldPointAwaitable when yield is OFF: await_ready==true)
struct YieldNoop {
  bool await_ready() const noexcept { return true; }

  void await_suspend(std::coroutine_handle<>) const noexcept {}

  void await_resume() const noexcept {}
};

// ───────────────────────── DESIGN: legacy (virtual Pull) ─────────────────────────
struct CursorL {
  uint64_t out_{0};
  virtual bool Pull() = 0;
  virtual ~CursorL() = default;
};

struct SourceL : CursorL {
  uint64_t i = 0, N;

  explicit SourceL(uint64_t n) : N(n) {}

  bool Pull() override {
    if (i >= N) return false;
    out_ = i++;
    return true;
  }
};

struct ExpandL : CursorL {
  CursorL *in;
  uint64_t F, base = 0, f = 0;
  bool have = false;

  ExpandL(CursorL *i, uint64_t ff) : in(i), F(ff) {}

  bool Pull() override {
    for (;;) {
      if (have && f < F) {
        out_ = mkval(base, f);
        ++f;
        return true;
      }
      if (!in->Pull()) return false;
      base = in->out_;
      have = true;
      f = 0;
    }
  }
};

static uint64_t run_legacy(uint64_t N, uint64_t F, int W) {
  SourceL s(N);
  ExpandL e(&s, F);
  uint64_t rows = 0;
  while (e.Pull()) {
    g_sink ^= do_work(e.out_, W);
    ++rows;
  }
  return rows;
}

// ───────────────────────── DESIGN: fused (single coroutine) ─────────────────────────
static PullAwaitable fused_gen(uint64_t N, uint64_t F, uint64_t *out) {
  for (uint64_t k = 0; k < N; ++k)
    for (uint64_t f = 0; f < F; ++f) {
      *out = mkval(k, f);
      co_yield true;
    }
  co_return false;
}

static uint64_t run_fused(uint64_t N, uint64_t F, int W) {
  uint64_t out = 0, rows = 0;
  PullAwaitable gen = fused_gen(N, F, &out);
  for (;;) {
    auto ra = gen.Resume();
    if (ResumePullStep(ra) != Status::Row) break;
    g_sink ^= do_work(out, W);
    ++rows;
  }
  return rows;
}

// ───────────────────────── DESIGN: allcoro / mixed / helper / allcoroY ─────────────────────────
struct CursorC {
  uint64_t out_{0};
  std::optional<PullAwaitable> gen_;
  virtual PullAwaitable DoPull() = 0;

  virtual PullAwaitable::ResumeAwaitable PullCo() {  // converted default
    if (!gen_) gen_ = DoPull();
    return gen_->Resume();
  }

  virtual ~CursorC() = default;
};

static inline PullAwaitable::ResumeAwaitable PullChild(CursorC &c) { return c.PullCo(); }

struct SourceC : CursorC {
  uint64_t N;

  explicit SourceC(uint64_t n) : N(n) {}

  PullAwaitable DoPull() override {
    for (uint64_t k = 0; k < N; ++k) {
      out_ = k;
      co_yield true;
    }
    co_return false;
  }
};

// leaf on the frame-less immediate path (selective conversion)
struct SourceImm : CursorC {
  uint64_t i = 0, N;

  explicit SourceImm(uint64_t n) : N(n) {}

  bool PullSync() {
    if (i >= N) return false;
    out_ = i++;
    return true;
  }

  PullAwaitable::ResumeAwaitable PullCo() override { return PullAwaitable::ResumeAwaitable::Immediate(PullSync()); }

  PullAwaitable DoPull() override { co_return false; }  // unused
};

template <bool YIELD>
struct ExpandC : CursorC {
  CursorC *in;
  uint64_t F, base = 0, f = 0;
  bool have = false;

  ExpandC(CursorC *i, uint64_t ff) : in(i), F(ff) {}

  PullAwaitable DoPull() override {
    for (;;) {
      if constexpr (YIELD) co_await YieldNoop{};
      if (have && f < F) {
        out_ = mkval(base, f);
        ++f;
        co_yield true;
        continue;
      }
      if (!co_await PullChild(*in)) co_return false;
      base = in->out_;
      have = true;
      f = 0;
    }
  }
};

// Expand that pulls its input through a ONE-SHOT helper coroutine (fresh frame per call): InitEdgesCo twin.
static PullAwaitable PullInputHelper(CursorC *in, uint64_t *base) {
  if (!co_await PullChild(*in)) co_return false;
  *base = in->out_;
  co_return true;
}

struct ExpandCHelper : CursorC {
  CursorC *in;
  uint64_t F, base = 0, f = 0;
  bool have = false;

  ExpandCHelper(CursorC *i, uint64_t ff) : in(i), F(ff) {}

  PullAwaitable DoPull() override {
    for (;;) {
      if (have && f < F) {
        out_ = mkval(base, f);
        ++f;
        co_yield true;
        continue;
      }
      if (!co_await PullInputHelper(in, &base)) co_return false;  // fresh frame each call
      have = true;
      f = 0;
    }
  }
};

// pass-through coroutine cursor (1:1, like Produce/Filter-pass): co_await child, co_yield it up.
// Used to build a pipeline of configurable DEPTH to measure the per-boundary crossing cost.
struct PassthroughC : CursorC {
  CursorC *in;

  explicit PassthroughC(CursorC *i) : in(i) {}

  PullAwaitable DoPull() override {
    for (;;) {
      if (!co_await PullChild(*in)) co_return false;
      out_ = in->out_;
      co_yield true;
    }
  }
};

template <typename Expand>
static uint64_t drive(Expand &e, int W) {
  uint64_t rows = 0;
  for (;;) {
    auto ra = e.PullCo();
    if (ResumePullStep(ra) != Status::Row) break;
    g_sink ^= do_work(e.out_, W);
    ++rows;
  }
  return rows;
}

static uint64_t run_allcoro(uint64_t N, uint64_t F, int W) {
  SourceC s(N);
  ExpandC<false> e(&s, F);
  return drive(e, W);
}

static uint64_t run_allcoroY(uint64_t N, uint64_t F, int W) {
  SourceC s(N);
  ExpandC<true> e(&s, F);
  return drive(e, W);
}

static uint64_t run_mixed(uint64_t N, uint64_t F, int W) {
  SourceImm s(N);
  ExpandC<false> e(&s, F);
  return drive(e, W);
}

static uint64_t run_helper(uint64_t N, uint64_t F, int W) {
  SourceC s(N);
  ExpandCHelper e(&s, F);
  return drive(e, W);
}

// DEPTH sweep: SourceImm (leaf, immediate) -> D pass-through COROUTINE cursors -> driver.
// Crossings per row == D (the immediate source pull is frame-less). Slope over D == per-crossing cost.
static uint64_t run_depth(uint64_t N, uint64_t D, int W) {
  SourceImm s(N);
  std::vector<std::unique_ptr<PassthroughC>> stack;
  CursorC *cur = &s;
  for (uint64_t i = 0; i < D; ++i) {
    stack.push_back(std::make_unique<PassthroughC>(cur));
    cur = stack.back().get();
  }
  uint64_t rows = 0;
  for (;;) {
    auto ra = cur->PullCo();
    if (ResumePullStep(ra) != Status::Row) break;
    g_sink ^= do_work(cur->out_, W);
    ++rows;
  }
  return rows;
}

// ───────────────────────── DESIGN: breaker (coroutine pipeline-breaker over a REGULAR pipeline) ─────────
// Models the proposed hybrid: a pipeline BREAKER (aggregate/sort) is a coroutine that yields mid-pull
// (during its pull-all), but the scan→expand pipeline FEEDING it stays REGULAR (virtual Pull, legacy
// speed). The breaker yields BETWEEN complete input pulls, so the regular pipeline never relays a yield.
// Per-input-row cost should be ~legacy (no per-row coroutine crossing), with yield still available.
static uint64_t g_consumed = 0;

static PullAwaitable breaker_gen(CursorL *top, int W) {
  while (true) {
    co_await YieldNoop{};  // the breaker's own mid-pull yield point (free when not yielding; suspends 1 frame when it
                           // does)
    if (!top->Pull()) break;
    g_sink ^= do_work(top->out_, W);
    ++g_consumed;
  }
  co_return true;  // breaker produced its single aggregate row
}

static uint64_t run_breaker(uint64_t N, uint64_t F, int W) {
  SourceL s(N);
  ExpandL e(&s, F);  // REGULAR input pipeline (virtual Pull)
  g_consumed = 0;
  PullAwaitable gen = breaker_gen(&e, W);
  auto ra = gen.Resume();
  ResumePullStep(ra);  // drive the breaker to completion (one output row); it consumes all input internally
  return g_consumed;   // report per-INPUT-row cost
}

// breaker variant: yield-check only every K rows (inner non-coroutine loop) -> hot loop stays register-resident.
static PullAwaitable breakerB_gen(CursorL *top, int W) {
  constexpr int K = 256;
  for (;;) {
    co_await YieldNoop{};  // amortized: one suspend point per K consumed rows
    bool done = false;
    for (int i = 0; i < K; ++i) {
      if (!top->Pull()) {
        done = true;
        break;
      }
      g_sink ^= do_work(top->out_, W);
      ++g_consumed;
    }
    if (done) break;
  }
  co_return true;
}

static uint64_t run_breakerB(uint64_t N, uint64_t F, int W) {
  SourceL s(N);
  ExpandL e(&s, F);
  g_consumed = 0;
  PullAwaitable gen = breakerB_gen(&e, W);
  auto ra = gen.Resume();
  ResumePullStep(ra);
  return g_consumed;
}

// ───────────────────────── EXTERNAL-YIELD (I/O park) model ─────────────────────────
// Models the on-disk future: a cold-page access deep in the scan parks the task; the scheduler
// runs something else; the task resumes when I/O completes. Here the "I/O" completes instantly so
// we measure ONLY the park+resume machinery cost. g_yield_period = yield every Nth access (1 =
// every pull = every access cold; 0 = never). Mirrors YieldPointAwaitable + ResumePullStep's
// stash-the-leaf-handle / resume-the-leaf protocol.
static uint64_t g_yield_period = 0;
static uint64_t g_yield_cnt = 0;
static std::coroutine_handle<> g_yield_slot{};
static int g_io_work = 0;  // simulated I/O latency per park (do_work iterations); ~2000 ~= ~1us

struct ExternalYield {
  bool await_ready() noexcept {
    if (!g_yield_period) return true;
    if (++g_yield_cnt < g_yield_period) return true;
    g_yield_cnt = 0;
    return false;  // suspend: park
  }

  void await_suspend(std::coroutine_handle<> h) noexcept { g_yield_slot = h; }  // stash leaf (void: unwind to driver)

  void await_resume() noexcept {}
};

// root driver that honours parks: on a park, resume the stashed leaf directly (== scheduler resuming
// the parked task after I/O). The leaf symmetric-transfers back up to the root on completion.
static inline Status ResumePullStepY(PullAwaitable::ResumeAwaitable &ra) {
  if (ra.Done()) return ra.Result() ? Status::Row : Status::Done;
  std::coroutine_handle<> t = ra.handle_;
  for (;;) {
    if (g_yield_slot) {
      t = g_yield_slot;
      g_yield_slot = {};
      if (g_io_work) g_sink ^= do_work(g_yield_cnt + 1, g_io_work);  // simulate the I/O wait before resuming
    }
    t.resume();
    if (g_yield_slot) continue;  // parked again (would return to scheduler; here resume immediately)
    break;
  }
  return ra.Result() ? Status::Row : Status::Done;
}

struct SourceEY : CursorC {  // leaf with an I/O-yield point inside the scan
  uint64_t N;

  explicit SourceEY(uint64_t n) : N(n) {}

  PullAwaitable DoPull() override {
    for (uint64_t k = 0; k < N; ++k) {
      co_await ExternalYield{};
      out_ = k;
      co_yield true;
    }
    co_return false;
  }
};

static PullAwaitable fused_gen_ey(uint64_t N, uint64_t F, uint64_t *out) {
  for (uint64_t k = 0; k < N; ++k)
    for (uint64_t f = 0; f < F; ++f) {
      co_await ExternalYield{};
      *out = mkval(k, f);
      co_yield true;
    }
  co_return false;
}

static uint64_t run_allcoroEY(uint64_t N, uint64_t F, int W) {
  SourceEY s(N);
  ExpandC<false> e(&s, F);
  uint64_t rows = 0;
  for (;;) {
    auto ra = e.PullCo();
    if (ResumePullStepY(ra) != Status::Row) break;
    g_sink ^= do_work(e.out_, W);
    ++rows;
  }
  return rows;
}

static uint64_t run_fusedEY(uint64_t N, uint64_t F, int W) {
  uint64_t out = 0, rows = 0;
  PullAwaitable gen = fused_gen_ey(N, F, &out);
  for (;;) {
    auto ra = gen.Resume();
    if (ResumePullStepY(ra) != Status::Row) break;
    g_sink ^= do_work(out, W);
    ++rows;
  }
  return rows;
}

// ───────────────────────── DESIGN: leandepth (trimmed framework) ─────────────────────────
// Same persistent-gen + symmetric-transfer model, but the hot await path is stripped to the bone:
// no immediate-mode fields/branches, no done() checks, no exception plumbing. Promise = {has_more_,
// parent_}. Isolates how much of the per-crossing cost is FRAMEWORK overhead (vs cursor logic).
struct LeanGen {
  struct promise_type {
    bool has_more_{false};
    std::coroutine_handle<> parent_{std::noop_coroutine()};

    LeanGen get_return_object() noexcept { return LeanGen{std::coroutine_handle<promise_type>::from_promise(*this)}; }

    static constexpr std::suspend_always initial_suspend() noexcept { return {}; }

    struct ST {
      std::coroutine_handle<> c;

      bool await_ready() const noexcept { return false; }

      std::coroutine_handle<> await_suspend(std::coroutine_handle<>) const noexcept { return c; }

      void await_resume() const noexcept {}
    };

    auto final_suspend() noexcept { return ST{parent_}; }

    void return_value(bool h) noexcept { has_more_ = h; }

    auto yield_value(bool h) noexcept {
      has_more_ = h;
      return ST{parent_};
    }

    void unhandled_exception() noexcept { std::abort(); }
  };

  std::coroutine_handle<promise_type> h_{};
  LeanGen() = default;

  explicit LeanGen(std::coroutine_handle<promise_type> h) noexcept : h_(h) {}

  LeanGen(LeanGen &&o) noexcept : h_(std::exchange(o.h_, {})) {}

  LeanGen &operator=(LeanGen &&o) noexcept {
    if (this != &o) {
      if (h_) h_.destroy();
      h_ = std::exchange(o.h_, {});
    }
    return *this;
  }

  LeanGen(const LeanGen &) = delete;
  LeanGen &operator=(const LeanGen &) = delete;

  ~LeanGen() {
    if (h_) h_.destroy();
  }

  struct Resume {  // LEAN awaiter: no imm-mode, no done() check on the hot path
    std::coroutine_handle<promise_type> h_;

    bool await_ready() const noexcept { return false; }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> p) const noexcept {
      h_.promise().parent_ = p;
      return h_;
    }

    bool await_resume() const noexcept { return h_.promise().has_more_; }
  };

  Resume ResumeA() noexcept { return Resume{h_}; }

  bool has_more() const noexcept { return h_.promise().has_more_; }

  bool done() const noexcept { return h_.done(); }
};

struct LeanCursor {
  uint64_t out_{0};
  std::optional<LeanGen> gen_;
  virtual LeanGen DoPull() = 0;

  LeanGen::Resume Pull_() {
    if (!gen_) gen_ = DoPull();
    return gen_->ResumeA();
  }

  virtual ~LeanCursor() = default;
};

static inline LeanGen::Resume LeanPullChild(LeanCursor &c) { return c.Pull_(); }

struct LeanSource : LeanCursor {
  uint64_t N;

  explicit LeanSource(uint64_t n) : N(n) {}

  LeanGen DoPull() override {
    for (uint64_t k = 0; k < N; ++k) {
      out_ = k;
      co_yield true;
    }
    co_return false;
  }
};

struct LeanPass : LeanCursor {
  LeanCursor *in;

  explicit LeanPass(LeanCursor *i) : in(i) {}

  LeanGen DoPull() override {
    for (;;) {
      if (!co_await LeanPullChild(*in)) co_return false;
      out_ = in->out_;
      co_yield true;
    }
  }
};

static uint64_t run_leandepth(uint64_t N, uint64_t D, int W) {
  LeanSource s(N);
  std::vector<std::unique_ptr<LeanPass>> st;
  LeanCursor *cur = &s;
  for (uint64_t i = 0; i < D; ++i) {
    st.push_back(std::make_unique<LeanPass>(cur));
    cur = st.back().get();
  }
  uint64_t rows = 0;
  // prime the generator
  (void)cur->Pull_();
  for (;;) {
    if (cur->gen_->done()) break;
    cur->gen_->h_.resume();
    if (!cur->gen_->has_more()) break;
    g_sink ^= do_work(cur->out_, W);
    ++rows;
  }
  return rows;
}

// ───────────────────────── main ─────────────────────────
int main(int argc, char **argv) {
  if (argc < 6) {
    std::fprintf(stderr, "usage: %s <design> <N> <F> <W> <reps>\n", argv[0]);
    std::fprintf(stderr, "  design: legacy|fused|allcoro|mixed|helper|allcoroY\n");
    return 2;
  }
  std::string design = argv[1];
  uint64_t N = std::strtoull(argv[2], nullptr, 10);
  uint64_t F = std::strtoull(argv[3], nullptr, 10);
  int W = std::atoi(argv[4]);
  uint64_t reps = std::strtoull(argv[5], nullptr, 10);
  g_yield_period = (argc > 6) ? std::strtoull(argv[6], nullptr, 10) : 0;  // external-yield period (EY designs)
  g_io_work = (argc > 7) ? std::atoi(argv[7]) : 0;                        // simulated I/O latency per park

  uint64_t (*fn)(uint64_t, uint64_t, int) = nullptr;
  if (design == "legacy")
    fn = run_legacy;
  else if (design == "fused")
    fn = run_fused;
  else if (design == "allcoro")
    fn = run_allcoro;
  else if (design == "allcoroY")
    fn = run_allcoroY;
  else if (design == "mixed")
    fn = run_mixed;
  else if (design == "helper")
    fn = run_helper;
  else if (design == "depth")
    fn = run_depth;  // F slot reinterpreted as pipeline depth D
  else if (design == "allcoroEY")
    fn = run_allcoroEY;  // external-yield (I/O park); period via argv[6]
  else if (design == "fusedEY")
    fn = run_fusedEY;
  else if (design == "breaker")
    fn = run_breaker;  // coroutine breaker over a REGULAR input pipeline (the proposed hybrid)
  else if (design == "breakerB")
    fn = run_breakerB;  // breaker with yield-check amortized every K rows
  else if (design == "leandepth")
    fn = run_leandepth;  // trimmed-framework depth sweep (F slot == depth D)
  else {
    std::fprintf(stderr, "unknown design: %s\n", design.c_str());
    return 2;
  }

  uint64_t total_rows = 0;
  auto t0 = std::chrono::steady_clock::now();
  for (uint64_t r = 0; r < reps; ++r) total_rows += fn(N, F, W);
  auto t1 = std::chrono::steady_clock::now();
  double ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();

  std::printf("design=%-9s N=%llu F=%llu W=%d reps=%llu rows=%llu ns/row=%.3f sink=%llu\n",
              design.c_str(),
              (unsigned long long)N,
              (unsigned long long)F,
              W,
              (unsigned long long)reps,
              (unsigned long long)total_rows,
              ns / (double)total_rows,
              g_sink);
  return 0;
}
