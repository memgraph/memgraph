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
#include <cstdint>
#include <mutex>
#include <nlohmann/json_fwd.hpp>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>

namespace memgraph::utils {
struct SafeString {
 private:
  // Process-wide source of version tokens: a token is never reused, so equal tokens mean the same object
  // with the same value, even if another SafeString later reuses this one's address.
  static uint64_t NextVersion() noexcept {
    static constinit std::atomic<uint64_t> next{1};  // 0 is never issued: callers can use it as "no value"
    return next.fetch_add(1, std::memory_order_relaxed);
  }

 public:
  SafeString() {}

  SafeString(std::string str) : str_(std::move(str)) {}

  SafeString(std::string_view str) : str_(str) {}

  SafeString(char const *str) : str_(str) {}

  SafeString(SafeString const &other) : str_(other.str()) {}

  SafeString(SafeString &&other) noexcept : str_(other.str()) {}

  SafeString &operator=(SafeString const &other) {
    if (this == &other) return *this;
    auto other_str = other.str();
    std::unique_lock lock(mutex_);
    str_ = std::move(other_str);
    Bump();
    return *this;
  }

  SafeString &operator=(SafeString &&other) noexcept {
    if (this == &other) return *this;
    std::scoped_lock lock(mutex_, other.mutex_);
    str_ = std::move(other.str_);
    Bump();
    other.Bump();
    return *this;
  }

  SafeString &operator=(std::string str) {
    std::unique_lock lock(mutex_);
    str_ = std::move(str);
    Bump();
    return *this;
  }

  SafeString &operator=(const char *str) {
    std::unique_lock lock(mutex_);
    str_ = str;
    Bump();
    return *this;
  }

  SafeString &operator=(std::string_view str) {
    std::unique_lock lock(mutex_);
    str_ = str;
    Bump();
    return *this;
  }

  std::string str() const {
    std::shared_lock lock(mutex_);
    return str_;
  }

  // Copies the value into `cache` unless `cache_version` shows it is already current. Lock-free when
  // unchanged, so a hot reader avoids str()'s shared_mutex RMW. `cache_version` starts at 0.
  void CopyIfChanged(std::string &cache, uint64_t &cache_version) const {
    if (version_.load(std::memory_order_acquire) == cache_version) return;
    std::shared_lock lock(mutex_);
    cache = str_;
    cache_version = version_.load(std::memory_order_relaxed);
  }

  // Takes the value out under the lock, leaving this empty.
  std::string move() {
    std::unique_lock lock(mutex_);
    std::string taken = std::exchange(str_, {});
    Bump();
    return taken;
  }

  struct ConstSafeWrapper {
    ConstSafeWrapper(const SafeString &str) : safe_str_(str), lock_(str.mutex_) {}

    const std::string &operator*() const { return safe_str_.str_; }

   private:
    const SafeString &safe_str_;
    std::shared_lock<std::shared_mutex> lock_;
  };

  ConstSafeWrapper str_view() const { return {*this}; }

  friend bool operator==(const SafeString &lrh, const SafeString &rhs) {
    if (&lrh == &rhs) return true;
    std::scoped_lock lock{lrh.mutex_, rhs.mutex_};
    return lrh.str_ == rhs.str_;
  }

  friend void to_json(nlohmann::json &data, SafeString const &str);

  friend void from_json(const nlohmann::json &data, SafeString &str);

 private:
  // Caller holds mutex_ exclusively.
  void Bump() noexcept { version_.store(NextVersion(), std::memory_order_release); }

  std::string str_;
  mutable std::shared_mutex mutex_;
  std::atomic<uint64_t> version_{NextVersion()};
};
}  // namespace memgraph::utils

namespace memgraph::slk {
class Reader;
class Builder;

void Save(const ::memgraph::utils::SafeString &self, Builder *builder);
void Load(::memgraph::utils::SafeString *self, Reader *reader);
}  // namespace memgraph::slk
