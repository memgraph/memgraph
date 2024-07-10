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

#include <shared_mutex>

namespace memgraph::query::procedure {

class Module;  // fwd-declare

/// Proxy for a registered Module, acquires a read lock for the module it points to.
class ModulePtr final {
  const Module *module_{nullptr};
  std::shared_lock<std::shared_timed_mutex> lock_;

 public:
  ModulePtr() = default;
  explicit ModulePtr(std::nullptr_t) {}
  ModulePtr(const Module *module, std::shared_lock<std::shared_timed_mutex> lock)
      : module_(module), lock_(std::move(lock)) {}

  ModulePtr(ModulePtr &&other) noexcept : module_(other.module_), lock_(std::move(other.lock_)) {
    other.module_ = nullptr;
  }

  ModulePtr &operator=(ModulePtr &&other) noexcept {
    if (this != &other) {
      module_ = other.module_;
      lock_ = std::move(other.lock_);
      other.module_ = nullptr;
    }
    return *this;
  }

  explicit operator bool() const { return static_cast<bool>(module_); }

  const Module &operator*() const { return *module_; }
  const Module *operator->() const { return module_; }
};

}  // namespace memgraph::query::procedure
