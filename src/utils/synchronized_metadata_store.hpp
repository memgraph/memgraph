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

#include <mutex>
#include <shared_mutex>
#include <unordered_set>

#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::utils {

template <typename T>
class SynchronizedMetaDataStore {
 public:
  SynchronizedMetaDataStore() = default;
  ~SynchronizedMetaDataStore() = default;

  SynchronizedMetaDataStore(const SynchronizedMetaDataStore &) = delete;
  SynchronizedMetaDataStore(SynchronizedMetaDataStore &&) = delete;
  SynchronizedMetaDataStore &operator=(const SynchronizedMetaDataStore &) = delete;
  SynchronizedMetaDataStore &operator=(SynchronizedMetaDataStore &&) = delete;

  void try_insert(const T &elem) {
    {
      std::shared_lock read_lock(lock_);
      if (element_store_.contains(elem)) {
        return;
      }
    }
    {
      std::unique_lock write_lock(lock_);
      element_store_.insert(elem);
    }
  }

  void erase(const T &elem) {
    std::unique_lock write_lock(lock_);
    element_store_.erase(elem);
  }

  template <typename TFunc>
  void for_each(const TFunc &func) {
    std::unique_lock write_lock(lock_);
    for (const auto &elem : element_store_) {
      func(elem);
    }
  }

 private:
  std::unordered_set<T> element_store_;
  RWLock lock_{RWLock::Priority::READ};
};

}  // namespace memgraph::utils
