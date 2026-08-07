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

#pragma once

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <type_traits>
#include <utility>

#include "utils/logging.hpp"

namespace memgraph::utils {

struct run_t {};

struct not_run_t {};

template <typename Ret>
struct EvalResult;

template <>
struct EvalResult<void> {
  template <typename Func, typename T>
  EvalResult(run_t /* marker */, Func &&func, T &arg) : was_run{true} {
    std::invoke(std::forward<Func>(func), arg);
  }

  EvalResult(not_run_t /* marker */) : was_run{false} {}

  ~EvalResult() = default;

  EvalResult(EvalResult const &) = delete;
  EvalResult(EvalResult &&) = delete;
  EvalResult &operator=(EvalResult const &) = delete;
  EvalResult &operator=(EvalResult &&) = delete;

  explicit operator bool() const { return was_run; }

 private:
  bool was_run;
};

template <typename Ret>
struct EvalResult {
  template <typename Func, typename T>
  EvalResult(run_t /* marker */, Func &&func, T &arg) : return_result{std::invoke(std::forward<Func>(func), arg)} {}

  EvalResult(not_run_t /* marker */) {}

  ~EvalResult() = default;

  EvalResult(EvalResult const &) = delete;
  EvalResult(EvalResult &&) = delete;
  EvalResult &operator=(EvalResult const &) = delete;
  EvalResult &operator=(EvalResult &&) = delete;

  explicit operator bool() const { return return_result.has_value(); }

  constexpr const Ret &value() const & { return return_result.value(); }

  constexpr Ret &value() & { return return_result.value(); }

  constexpr Ret &&value() && { return return_result.value(); }

  constexpr const Ret &&value() const && { return return_result.value(); }

 private:
  std::optional<Ret> return_result = std::nullopt;
};

template <typename Func, typename T>
EvalResult(run_t, Func &&, T &) -> EvalResult<std::invoke_result_t<Func, T &>>;

// Opt-in RAII guard that Gatekeeper installs around the lifetime of the
// managed object.  Types can declare a nested `GatekeeperGuard` to have
// Gatekeeper construct it before the object and destroy it after.
template <typename T, typename = void>
struct GatekeeperGuardFor {
  struct type {};
};

template <typename T>
struct GatekeeperGuardFor<T, std::void_t<typename T::GatekeeperGuard>> {
  using type = typename T::GatekeeperGuard;
};

// 4-state lifecycle for hot/cold tenant management.
// Transitions (all under GKInternals::mutex_):
//   HOT -> SUSPENDING  : Gatekeeper::try_begin_suspend()   — sole-accessor gate
//   SUSPENDING -> HOT  : Gatekeeper::abort_suspend()        — rollback
//   SUSPENDING -> COLD : Gatekeeper::finish_suspend()       — value destroyed, shell left
//   COLD -> RESUMING   : Gatekeeper::begin_resume()         — single-flight token
//   RESUMING -> COLD   : Gatekeeper::abort_resume()         — resume failed, retry allowed
//   RESUMING -> HOT    : move-assign a fresh HOT Gatekeeper over the shell
// INVARIANT: Gatekeeper::access() mints Accessors only in HOT.
//
// DRAINING (GKInternals::draining_, set by Gatekeeper::begin_drain()) is an orthogonal flag layered
// on top of HOT, NOT a fifth state: it is set/cleared without moving state_, so it never appears in
// the transition table above and cannot desynchronize from it.
//
// Declared at namespace scope so headers that forward-declare the managed type
// T can use GatekeeperState without forcing T to be complete.
enum class GatekeeperState : uint8_t { HOT, SUSPENDING, COLD, RESUMING };

// Tag to construct a Gatekeeper directly as a COLD shell (no managed value): used by hot/cold
// cross-restart recovery to bring a previously-suspended tenant back COLD without building its
// storage. The shell starts in COLD with count_ == 0, so a later begin_resume()/move-assign-HOT
// completes the lazy reheat exactly as a runtime suspend would, and ~Gatekeeper destroys it cleanly
// (its wait predicate — terminal state + drained count — is satisfied immediately).
struct cold_shell_t {
  explicit cold_shell_t() = default;
};

inline constexpr cold_shell_t cold_shell{};

// Tag to bypass the DRAINING refusal in access() below (see Gatekeeper::access(drain_bypass_t)'s
// doc comment for exactly who may pass it and why).
struct drain_bypass_t {
  explicit drain_bypass_t() = default;
};

inline constexpr drain_bypass_t drain_bypass{};

template <typename T>
struct GKInternals {
  template <typename... Args>
  explicit GKInternals(Args &&...args) : value_{std::make_unique<T>(std::forward<Args>(args)...)} {}

  // COLD-shell construction: value_ stays null, state_ starts COLD. Non-template so it is preferred
  // over the variadic ctor for an exact cold_shell_t argument (which would otherwise try
  // make_unique<T>(cold_shell) and fail to compile for managed types with no such constructor).
  explicit GKInternals(cold_shell_t /*tag*/) : state_{GatekeeperState::COLD} {}

  std::unique_ptr<T> value_;
  uint64_t count_ = 0;
  std::atomic_bool is_marked_for_deletion = false;
  // Plain bool, NOT atomic: every access happens under mutex_ already, and begin_drain() needs the
  // HOT check and the set to be one indivisible step — an atomic<bool> gives atomicity to each
  // access individually but not to "check state_ == HOT, then set draining_" as a single unit.
  bool draining_ = false;
  std::mutex mutex_;  // TODO change to something cheaper?
  std::condition_variable cv_;
  GatekeeperState state_ = GatekeeperState::HOT;
};

template <typename T>
struct Gatekeeper {
  template <typename... Args>
  explicit Gatekeeper(Args &&...args) {
    // Opt-in lifetime guard around object construction.
    // Types without a nested GatekeeperGuard get a zero-size no-op.
    [[maybe_unused]] typename GatekeeperGuardFor<T>::type guard;
    pimpl_ = std::make_unique<GKInternals<T>>(std::forward<Args>(args)...);
  }

  Gatekeeper(Gatekeeper const &) = delete;
  Gatekeeper(Gatekeeper &&) noexcept = default;
  Gatekeeper &operator=(Gatekeeper const &) = delete;
  // LOAD-BEARING for hot/cold deadlock-freedom: the defaulted move-assign tears the OLD state down by
  // resetting `pimpl_` (a unique_ptr), which runs ~GKInternals — a plain, NON-blocking destructor. It
  // does NOT run ~Gatekeeper (whose teardown blocks until count == 0 + a terminal state). DbmsHandler's
  // RESUME publish (`*gk = std::move(fresh)`) relies on this: it overwrites a RESUMING shell while
  // holding the handler lock_, and would deadlock if overwriting instead invoked the blocking
  // ~Gatekeeper. Keep this `= default` (and keep ~GKInternals non-blocking) or that publish path hangs.
  Gatekeeper &operator=(Gatekeeper &&) noexcept = default;

  struct Accessor {
    friend Gatekeeper;

   private:
    explicit Accessor(Gatekeeper *owner) : owner_{owner->pimpl_.get()} { ++owner_->count_; }

   public:
    // CONTRACT: copying bumps count_ but does NOT re-check state_. Copying the *sole* live accessor
    // while a suspend is in flight (SUSPENDING) would push count_ 1->2 and break the sole-accessor
    // invariant try_begin_suspend() established, leading to a dangling value_ after finish_suspend().
    // Safe today because access() returns nullopt outside HOT, so no new accessor can be minted
    // during SUSPENDING, and the suspend path's own accessor is a stack-local that is never copied.
    // The read of other.owner_ then lock of owner_->mutex_ is also safe WITHOUT a prior lock: the
    // source `other` holds a live count on the same GKInternals, so count_ >= 1 throughout this copy
    // and ~Gatekeeper (which frees GKInternals only at count_ == 0) cannot destroy owner_ underneath
    // us. This relies on the caller NOT destroying/sharing `other` on another thread mid-copy.
    Accessor(Accessor const &other) : owner_{other.owner_} {
      if (owner_) {
        auto guard = std::unique_lock{owner_->mutex_};
        ++owner_->count_;
      }
    };

    Accessor(Accessor &&other) noexcept : owner_{std::exchange(other.owner_, nullptr)} {};

    Accessor &operator=(Accessor const &other) {
      // no change assignment
      if (owner_ == other.owner_) {
        return *this;
      }

      // gain ownership
      if (other.owner_) {
        auto guard = std::unique_lock{other.owner_->mutex_};
        ++other.owner_->count_;
      }

      // reliquish ownership
      if (owner_) {
        auto guard = std::unique_lock{owner_->mutex_};
        --owner_->count_;
        owner_->cv_.notify_all();
      }

      // correct owner
      owner_ = other.owner_;
      return *this;
    };

    Accessor &operator=(Accessor &&other) noexcept {
      // self assignment
      if (&other == this) return *this;

      // reliquish ownership
      if (owner_) {
        auto guard = std::unique_lock{owner_->mutex_};
        --owner_->count_;
        owner_->cv_.notify_all();
      }

      // correct owners
      owner_ = std::exchange(other.owner_, nullptr);
      return *this;
    }

    [[nodiscard]] bool is_marked_for_deletion() const { return owner_ && owner_->is_marked_for_deletion; }

    void prepare_for_deletion() {
      if (owner_) {
        owner_->is_marked_for_deletion = true;
      }
    }

    ~Accessor() { reset(); }

    auto get() -> T * {
      if (owner_ == nullptr) return nullptr;
      return owner_->value_.get();
    }

    auto get() const -> const T * {
      if (owner_ == nullptr) return nullptr;
      return owner_->value_.get();
    }

    T *operator->() {
      if (owner_ == nullptr) return nullptr;
      return owner_->value_.get();
    }

    const T *operator->() const {
      if (owner_ == nullptr) return nullptr;
      return owner_->value_.get();
    }

    template <typename Func>
    [[nodiscard]] auto try_exclusively(Func &&func) -> EvalResult<std::invoke_result_t<Func, T &>> {
      if (!owner_) return {not_run_t{}};
      // Prevent new access
      auto guard = std::unique_lock{owner_->mutex_};
      // Only invoke if we have exclusive access and there is still a value (try_delete()'s unlocked
      // destruction window briefly holds count_ == 1 with value_ == nullptr).
      if (owner_->count_ != 1 || !owner_->value_) {
        return {not_run_t{}};
      }
      // Invoke and hold result in wrapper type
      return {run_t{}, std::forward<Func>(func), *owner_->value_};
    }

    // Completely invalidates the accessor if it returns true.
    //
    // ~T runs AFTER mutex_ is released (see the pointer move below), never under it: ~T (e.g.
    // ~Database -> ~InMemoryStorage -> StopAllBackgroundTasks()) joins background threads (async
    // indexer, TTL scheduler) that re-enter this gatekeeper through access() to mint their own
    // Accessor. If the destroying thread still held mutex_ here, that mint would block on the very
    // mutex the joiner is holding while it waits for the join -- an AB-BA deadlock reachable from a
    // plain, non-FORCE DROP DATABASE. Do NOT move the destruction back under the lock.
    template <typename Func = decltype([](T &) { return true; })>
    [[nodiscard]] bool try_delete(std::chrono::milliseconds timeout = std::chrono::milliseconds(100),
                                  Func &&predicate = {}) {
      if (!owner_) return false;
      std::unique_ptr<T> dying;  // destroyed at scope exit below, AFTER mutex_ is released
      {
        // Prevent new access
        auto guard = std::unique_lock{owner_->mutex_};
        if (!owner_->cv_.wait_for(guard, timeout, [this] { return owner_->count_ == 1; })) {
          return false;
        }
        // Already deleted
        if (!owner_->value_) return true;
        // Delete value if ok
        if (!predicate(*owner_->value_)) return false;
        // Pointer move: transfers ownership without running ~T or moving T under the lock. Every
        // locked observer sees value_ == nullptr the instant this commits.
        dying = std::move(owner_->value_);
        owner_->cv_.notify_all();
      }  // <-- mutex_ released here
      // Opt-in lifetime guard around object destruction.
      [[maybe_unused]] typename GatekeeperGuardFor<T>::type arena_guard;
      dying.reset();  // ~T runs unlocked; its thread joins can now complete
      return true;
    }

    // Unlocked, advisory only: reads owner_ and the atomic is_marked_for_deletion without mutex_, so
    // it must NOT also read the non-atomic value_ (that would race try_delete()'s unlocked move-out).
    // May report true while get()/operator->() return nullptr — callers that need the value must
    // check the pointer, not rely on this alone.
    explicit operator bool() const {
      return owner_ != nullptr                    // we have access
             && !owner_->is_marked_for_deletion;  // AND we are allowed to use it
    }

    void reset() {
      if (owner_) {
        {
          auto guard = std::unique_lock{owner_->mutex_};
          --owner_->count_;
          owner_->cv_.notify_all();
        }
      }
      owner_ = nullptr;
    }

    friend bool operator==(Accessor const &lhs, Accessor const &rhs) { return lhs.owner_ == rhs.owner_; }

   private:
    GKInternals<T> *owner_ = nullptr;
  };

 private:
  // Shared mint condition for both access() overloads below; caller must hold pimpl_->mutex_.
  // Factored out so the plain and drain_bypass_t overloads cannot drift apart.
  std::optional<Accessor> access_locked(bool bypass_drain) {
    if (pimpl_->value_ && pimpl_->state_ == GatekeeperState::HOT && (bypass_drain || !pimpl_->draining_)) {
      return Accessor{this};
    }
    return std::nullopt;
  }

 public:
  std::optional<Accessor> access() {
    auto guard = std::unique_lock{pimpl_->mutex_};
    // Intentionally gated ONLY on state_ == HOT, NOT on is_marked_for_deletion: a tenant being deleted
    // can still be HOT, and the marked-for-deletion guard is surfaced via Accessor::operator bool (the
    // caller checks it and releases). access() minting on a marked-but-HOT shell is benign — the minted
    // Accessor's operator bool is false, so callers won't use it, and the count returns to 0 so a
    // waiting ~Gatekeeper proceeds. Adding a marked check here is NOT a correctness requirement.
    //
    // draining_ is different: it IS a hard refusal, not advisory. It is the single choke point that
    // stops the database-protector factory (dbms::DatabaseHandler::MakeDatabaseProtectorFactory ->
    // Handler::Get -> here) from re-arming TTL and async-index work on a tenant that is being dropped
    // — without this refusal the drain would never converge, because freshly-minted work would keep
    // the tenant HOT and its own Accessor count above the drop's single-flight expectations forever.
    return access_locked(/*bypass_drain=*/false);
  }

  // Bypasses the draining_ refusal above. Restricted to deletion/teardown machinery:
  // Handler<T>::DeferDelete, and DatabaseHandler's destructor / its New() directory-collision scan.
  // Mandatory, not a convenience: Handler<T>::DeferDelete does
  //   auto db_acc = itr->second.access(); if (!db_acc) return;
  // *before* its unconditional items_.erase(itr) — a drain-gated mint there would strand the tenant
  // in items_ forever: unreachable by name, never destroyed, its name permanently unusable.
  // Do NOT use this from anything that could win Accessor::try_delete() behind the drop's back —
  // that is exactly why Handler<T>::TryDelete stays gated on the non-bypass overload above.
  std::optional<Accessor> access(drain_bypass_t /*tag*/) {
    auto guard = std::unique_lock{pimpl_->mutex_};
    return access_locked(/*bypass_drain=*/true);
  }

  std::optional<bool> is_marked_for_deletion() const {
    auto guard = std::unique_lock{pimpl_->mutex_};
    if (pimpl_->value_) {
      return pimpl_->is_marked_for_deletion;
    }
    return std::nullopt;
  }

  // Returns the current lifecycle state (locks mutex_).
  GatekeeperState state() const {
    auto guard = std::unique_lock{pimpl_->mutex_};
    return pimpl_->state_;
  }

  // Live-Accessor count at this instant — INHERENTLY RACY: Accessors mint/release under only
  // pimpl_->mutex_ (e.g. the database-protector factory takes no handler lock). Diagnostics only;
  // use try_delete()/try_begin_suspend(), which re-check count_ under this mutex, to actually gate.
  uint64_t holder_count() const {
    auto guard = std::unique_lock{pimpl_->mutex_};
    return pimpl_->count_;
  }

  // HOT -> SUSPENDING.
  // Waits up to `timeout` for the accessor count to drain to exactly 1 (i.e.
  // only the caller's own accessor is live). The caller MUST hold exactly one
  // live Accessor when calling this method. Returns false immediately if
  // state != HOT, or if the timeout expires while count != 1.  On success
  // state becomes SUSPENDING and access() will return nullopt for new callers.
  // The caller should release their Accessor and perform the actual teardown of
  // the managed value, then call finish_suspend().
  bool try_begin_suspend(std::chrono::milliseconds timeout = std::chrono::milliseconds(100)) {
    auto guard = std::unique_lock{pimpl_->mutex_};
    // draining_ is refused upfront, alongside the HOT check, NOT folded into the wait_for predicate
    // below (which must stay count_ == 1 only): a tenant already accepted for deletion must not be
    // frozen out from under the drop by a competing suspend. value_ is refused too: try_delete()
    // briefly holds count_ == 1 / state_ == HOT / draining_ == false while ~T runs unlocked after
    // moving value_ out, and a gatekeeper with nothing to suspend must not enter SUSPENDING.
    if (!pimpl_->value_ || pimpl_->state_ != GatekeeperState::HOT || pimpl_->draining_) return false;
    if (!pimpl_->cv_.wait_for(guard, timeout, [this] { return pimpl_->count_ == 1; })) {
      return false;
    }
    pimpl_->state_ = GatekeeperState::SUSPENDING;
    pimpl_->cv_.notify_all();
    return true;
  }

  // SUSPENDING -> HOT.
  // Reverses a freeze when the caller decides to abort the suspend.
  void abort_suspend() {
    auto guard = std::unique_lock{pimpl_->mutex_};
    // DMG_ASSERT (debug/CI-only): a debug regression tripwire, NOT a prod safety net — the wrong-state
    // call is unreachable on every current path (one disciplined caller per transition), and a
    // multi-tenant prod process must not crash on a state mismatch (release just mis-transitions).
    DMG_ASSERT(pimpl_->state_ == GatekeeperState::SUSPENDING, "abort_suspend() called outside SUSPENDING state");
    pimpl_->state_ = GatekeeperState::HOT;
    pimpl_->cv_.notify_all();
  }

  // SUSPENDING -> COLD.
  // Destroys the managed value (with opt-in GatekeeperGuard active) and
  // publishes the COLD shell.  After this call access() returns nullopt and
  // value_ is disengaged.
  void finish_suspend() {
    auto guard = std::unique_lock{pimpl_->mutex_};
    // DMG_ASSERT (debug/CI-only regression tripwire) — see abort_suspend() for the rationale + trade-off.
    DMG_ASSERT(pimpl_->state_ == GatekeeperState::SUSPENDING, "finish_suspend() called outside SUSPENDING state");
    {
      // Opt-in lifetime guard around object destruction (mirrors the dtor).
      [[maybe_unused]] typename GatekeeperGuardFor<T>::type arena_guard;
      // INTENTIONAL: pimpl_->mutex_ is held across value_.reset() (Database destruction +
      // WAL finalization).  This is the load-bearing suspend->resume directory handoff:
      // begin_resume() (which transitions COLD->RESUMING) also runs under pimpl_->mutex_,
      // so a concurrent Resume_ CANNOT start recovering from the on-disk directory until the
      // old Database has finished finalizing its WAL and all its files are closed.
      // Do NOT "optimize" this by releasing the mutex before destruction — doing so would
      // open a window where a concurrent begin_resume() starts reading the directory while
      // the old Database is still writing its final WAL segment.
      //
      // INVARIANT (load-bearing): T's destructor (here ~Database -> ~InMemoryStorage) MUST NOT
      // call any Gatekeeper method on the gatekeeper that owns it. pimpl_->mutex_ is non-recursive
      // and held here, so re-entry (e.g. a pre-destruction hook calling access()/try_*()) would
      // self-deadlock. Verified today: the ~Database/~InMemoryStorage chain takes no gatekeeper
      // path. Anyone adding a destruction-time DBMS callback must preserve this.
      DMG_ASSERT(pimpl_->count_ == 0, "finish_suspend() must not destroy value_ while accessors are live");
      pimpl_->value_.reset();
    }
    pimpl_->state_ = GatekeeperState::COLD;
    pimpl_->cv_.notify_all();
  }

  // COLD -> RESUMING (single-flight).
  // Only the first caller wins the COLD->RESUMING transition; concurrent
  // callers that already see RESUMING get false.  This acts as a single-flight
  // token: the winner is responsible for loading the value and then
  // move-assigning a freshly-built HOT Gatekeeper over this shell to complete
  // the transition.  Call abort_resume() if loading fails.
  bool begin_resume() {
    auto guard = std::unique_lock{pimpl_->mutex_};
    // NOT an assert (unlike the single-caller abort_suspend/abort_resume below): the mutex makes this
    // check-and-set atomic, which is exactly what makes it a single-flight token — it does NOT mean the
    // state is always COLD on entry. Concurrent resumers (e.g. a client RESUME racing a replica
    // ResumeDatabase apply) legitimately arrive here; the loser sees RESUMING (or an already-HOT racer)
    // and MUST get false so Resume_ takes its poll-until-HOT loser path. MG_ASSERT here would terminate
    // the process on a legal race.
    if (pimpl_->state_ != GatekeeperState::COLD) return false;
    pimpl_->state_ = GatekeeperState::RESUMING;
    pimpl_->cv_.notify_all();
    return true;
  }

  // RESUMING -> COLD.
  // Resume failed; rolls back to COLD so begin_resume() can be retried.
  void abort_resume() {
    auto guard = std::unique_lock{pimpl_->mutex_};
    // DMG_ASSERT (debug/CI-only regression tripwire) — see abort_suspend() for the rationale + trade-off.
    DMG_ASSERT(pimpl_->state_ == GatekeeperState::RESUMING, "abort_resume() called outside RESUMING state");
    pimpl_->state_ = GatekeeperState::COLD;
    pimpl_->cv_.notify_all();
  }

  // HOT -> HOT, draining_: false -> true. Deliberately NOT a GatekeeperState transition — see the
  // note on GatekeeperState above and on draining_ in GKInternals for why a fifth state value would
  // hang ~Gatekeeper's terminal predicate forever.
  //
  // Single-flight CAS *and* the concurrent-SUSPEND/RESUME guard in one atomic step: requires
  // state_ == HOT (refuses an in-flight SUSPENDING/RESUMING gatekeeper, and a COLD one) AND
  // !draining_ (a second concurrent DROP loses). Because draining_ can only ever be set here, from
  // HOT, it is HOT-exclusive for its entire life — that is exactly why ~Gatekeeper's terminal
  // predicate, begin_resume() (COLD-only), and EraseColdShell/DeleteCold_ (COLD-only) all stay valid
  // unchanged: none of them can ever observe draining_ == true.
  //
  // Returning false is a normal, non-asserting outcome (unlike abort_drain() below): a concurrent
  // drop or a mid-transition tenant is a legal race that the caller turns into a retriable error.
  bool begin_drain() {
    auto guard = std::unique_lock{pimpl_->mutex_};
    if (pimpl_->state_ != GatekeeperState::HOT || pimpl_->draining_) return false;
    pimpl_->draining_ = true;
    pimpl_->cv_.notify_all();
    return true;
  }

  // Rolls back begin_drain(). Single disciplined caller (unlike begin_drain()'s legal-race false), so
  // a DMG_ASSERT tripwire, matching abort_suspend()/abort_resume() above.
  void abort_drain() {
    auto guard = std::unique_lock{pimpl_->mutex_};
    DMG_ASSERT(pimpl_->draining_, "abort_drain() called while not draining");
    pimpl_->draining_ = false;
    pimpl_->cv_.notify_all();
  }

  bool is_draining() const {
    auto guard = std::unique_lock{pimpl_->mutex_};
    return pimpl_->draining_;
  }

  ~Gatekeeper() {
    if (!pimpl_) return;  // Moved out, nothing to do
    pimpl_->is_marked_for_deletion = true;
    // Wait for a terminal state (HOT or COLD) AND a drained accessor count.
    // A graceful destruction during SUSPENDING or RESUMING is a caller
    // ordering error — the owner must quiesce in-flight transitions before
    // destroying. The terminal-state guard is the backstop: every transition
    // above calls notify_all() so this wait cannot lose a wakeup.
    {
      auto lock = std::unique_lock{pimpl_->mutex_};
      auto const terminal_and_drained = [this] {
        // Deliberately tests state_ and count_ only — draining_ is an orthogonal flag (see
        // GatekeeperState's comment above), not a state, so it never enters this predicate and a
        // draining HOT/COLD gatekeeper stays destructible. Anyone adding a new GatekeeperState value
        // must extend this predicate too, or a Gatekeeper stuck in that state waits here forever.
        return (pimpl_->state_ == GatekeeperState::HOT || pimpl_->state_ == GatekeeperState::COLD) &&
               pimpl_->count_ == 0;
      };
      // The wait stays unbounded (a slow accessor drain on shutdown is legitimate); TRACE + a 5min
      // cadence is a quiet breadcrumb for a trace-enabled hang debug, not an alarming WARN for a benign drain.
      constexpr auto kDiagInterval = std::chrono::minutes{5};
      while (!pimpl_->cv_.wait_for(lock, kDiagInterval, terminal_and_drained)) {
        // spdlog can throw (formatting, sink I/O); a destructor is implicitly noexcept, so an escaping
        // exception would call std::terminate. Swallow it — the wait condition is what matters here.
        try {
          spdlog::trace(
              "~Gatekeeper has waited >5min for a terminal state and a drained accessor count (state={}, "
              "count={}). A graceful destruction during SUSPENDING/RESUMING is a caller ordering error — "
              "in-flight transitions must be quiesced before destroying.",
              static_cast<int>(pimpl_->state_),
              pimpl_->count_);
        } catch (...) {  // NOLINT(bugprone-empty-catch)
        }
      }
    }
    // Opt-in lifetime guard around object destruction.
    [[maybe_unused]] typename GatekeeperGuardFor<T>::type guard;
    pimpl_.reset();  // destroys GKInternals<T> (and thus T) while guard is active
  }

 private:
  std::unique_ptr<GKInternals<T>> pimpl_;
};

}  // namespace memgraph::utils
