// Copyright 2024 Memgraph Ltd.
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

#include <cassert>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <iosfwd>
#include <memory>
#include <mutex>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

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

template <typename T>
struct GKInternals {
  template <typename... Args>
  explicit GKInternals(Args &&...args) : value_{std::in_place, std::forward<Args>(args)...} {}

  std::optional<T> value_;
  uint64_t count_ = 0;
  std::atomic_bool is_deleting = false;
  std::mutex mutex_;  // TODO change to something cheaper?
  std::condition_variable cv_;
};

template <typename T>
struct Gatekeeper {
  template <typename... Args>
  explicit Gatekeeper(Args &&...args) : pimpl_(std::make_unique<GKInternals<T>>(std::forward<Args>(args)...)) {}

  Gatekeeper(Gatekeeper const &) = delete;
  Gatekeeper(Gatekeeper &&) noexcept = default;
  Gatekeeper &operator=(Gatekeeper const &) = delete;
  Gatekeeper &operator=(Gatekeeper &&) noexcept = default;

  struct Accessor {
    friend Gatekeeper;

   private:
    explicit Accessor(Gatekeeper *owner) : owner_{owner->pimpl_.get()} { ++owner_->count_; }

   public:
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
      }

      // correct owners
      owner_ = std::exchange(other.owner_, nullptr);
      return *this;
    }

    [[nodiscard]] bool is_deleting() const { return owner_->is_deleting; }

    void prepare_for_deletion() {
      if (owner_) {
        owner_->is_deleting = true;
      }
    }

    ~Accessor() { reset(); }

    auto get() -> T * {
      if (owner_ == nullptr) return nullptr;
      return std::addressof(*owner_->value_);
    }
    auto get() const -> const T * {
      if (owner_ == nullptr) return nullptr;
      return std::addressof(*owner_->value_);
    }
    T *operator->() {
      if (owner_ == nullptr) return nullptr;
      return std::addressof(*owner_->value_);
    }
    const T *operator->() const {
      if (owner_ == nullptr) return nullptr;
      return std::addressof(*owner_->value_);
    }

    template <typename Func>
    [[nodiscard]] auto try_exclusively(Func &&func) -> EvalResult<std::invoke_result_t<Func, T &>> {
      // Prevent new access
      auto guard = std::unique_lock{owner_->mutex_};
      // Only invoke if we have exclusive access
      if (owner_->count_ != 1) {
        return {not_run_t{}};
      }
      // Invoke and hold result in wrapper type
      return {run_t{}, std::forward<Func>(func), *owner_->value_};
    }

    // Completely invalidated the accessor if return true
    template <typename Func = decltype([](T &) { return true; })>
    [[nodiscard]] bool try_delete(std::chrono::milliseconds timeout = std::chrono::milliseconds(100),
                                  Func &&predicate = {}) {
      // Prevent new access
      auto guard = std::unique_lock{owner_->mutex_};
      if (!owner_->cv_.wait_for(guard, timeout, [this] { return owner_->count_ == 1; })) {
        return false;
      }
      // Already deleted
      if (owner_->value_ == std::nullopt) return true;
      // Delete value if ok
      if (!predicate(*owner_->value_)) return false;
      owner_->value_ = std::nullopt;
      return true;
    }

    explicit operator bool() const {
      return owner_ != nullptr         // we have access
             && !owner_->is_deleting;  // AND we are allowed to use it
    }

    void reset() {
      if (owner_) {
        {
          auto guard = std::unique_lock{owner_->mutex_};
          --owner_->count_;
        }
        owner_->cv_.notify_all();
      }
      owner_ = nullptr;
    }

    friend bool operator==(Accessor const &lhs, Accessor const &rhs) { return lhs.owner_ == rhs.owner_; }

   private:
    GKInternals<T> *owner_ = nullptr;
  };

  std::optional<Accessor> access() {
    auto guard = std::unique_lock{pimpl_->mutex_};
    if (pimpl_->value_) {
      return Accessor{this};
    }
    return std::nullopt;
  }

  ~Gatekeeper() {
    if (!pimpl_) return;  // Moved out, nothing to do
    pimpl_->is_deleting = true;
    // wait for count to drain to 0
    auto lock = std::unique_lock{pimpl_->mutex_};
    pimpl_->cv_.wait(lock, [this] { return pimpl_->count_ == 0; });
  }

 private:
  std::unique_ptr<GKInternals<T>> pimpl_;
};

}  // namespace memgraph::utils
