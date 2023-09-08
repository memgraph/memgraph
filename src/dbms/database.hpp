// Copyright 2023 Memgraph Ltd.
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

#include <algorithm>
#include <filesystem>
#include <iterator>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>

#include "query/cypher_query_interpreter.hpp"
#include "query/stream/streams.hpp"
#include "query/trigger.hpp"
#include "storage/v2/storage.hpp"

#include "handler.hpp"

namespace memgraph::dbms {

class Database {
 public:
  explicit Database(const storage::Config &config);

  storage::Storage *storage() { return storage_.get(); }  // TODO Remove
  std::unique_ptr<storage::Storage::Accessor> Access(
      std::optional<storage::IsolationLevel> override_isolation_level = {}) {
    return storage_->Access(override_isolation_level);
  }
  const std::string &id() const { return storage_->id(); }
  storage::StorageMode GetStorageMode() const { return storage_->GetStorageMode(); }
  storage::StorageInfo GetInfo() const { return storage_->GetInfo(); }
  void SwitchToOnDisk();

  query::TriggerStore *trigger_store() { return &trigger_store_; }

  query::stream::Streams *streams() { return &streams_; }

  utils::ThreadPool *thread_pool() { return &after_commit_trigger_pool_; }
  void AddTask(std::function<void()> new_task) { after_commit_trigger_pool_.AddTask(std::move(new_task)); }

  const storage::Config &config() const { return storage_->config_; }

  utils::SkipList<query::PlanCacheEntry> *plan_cache() { return &plan_cache_; }

 private:
  std::unique_ptr<storage::Storage> storage_;
  query::TriggerStore trigger_store_;
  utils::ThreadPool after_commit_trigger_pool_{1};
  query::stream::Streams streams_;

  // TODO: Move to a better place
  utils::SkipList<query::PlanCacheEntry> plan_cache_;
};

}  // namespace memgraph::dbms

extern template struct memgraph::utils::Gatekeeper<memgraph::dbms::Database>;

namespace memgraph::dbms {
using DatabaseAccess = memgraph::utils::Gatekeeper<memgraph::dbms::Database>::access;
}  // namespace memgraph::dbms
