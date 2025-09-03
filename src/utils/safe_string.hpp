// Copyright 2025 Memgraph Ltd.
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

#include <mutex>
#include <nlohmann/json_fwd.hpp>
#include <shared_mutex>
#include <string>

namespace memgraph::utils {
struct SafeString {
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
    return *this;
  }
  SafeString &operator=(SafeString &&other) noexcept {
    if (this == &other) return *this;
    std::scoped_lock lock(mutex_, other.mutex_);
    str_ = std::move(other.str_);
    return *this;
  }

  SafeString &operator=(std::string str) {
    std::unique_lock lock(mutex_);
    str_ = std::move(str);
    return *this;
  }

  SafeString &operator=(const char *str) {
    std::unique_lock lock(mutex_);
    str_ = str;
    return *this;
  }

  SafeString &operator=(std::string_view str) {
    std::unique_lock lock(mutex_);
    str_ = str;
    return *this;
  }

  std::string str() const {
    std::shared_lock lock(mutex_);
    return str_;
  }

  std::string &&move() {
    std::unique_lock lock(mutex_);
    return std::move(str_);
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
  std::string str_;
  mutable std::shared_mutex mutex_;
};
}  // namespace memgraph::utils

namespace memgraph::slk {
class Reader;
class Builder;
void Save(const ::memgraph::utils::SafeString &self, Builder *builder);
void Load(::memgraph::utils::SafeString *self, Reader *reader);
}  // namespace memgraph::slk
