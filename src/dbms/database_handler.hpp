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

#ifdef MG_ENTERPRISE

#include <algorithm>
#include <filesystem>
#include <iterator>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>

#include "query/interpreter.hpp"
#include "query/stream/streams.hpp"
#include "query/trigger.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"

#include "handler.hpp"

namespace memgraph::dbms {

// class StorageWrapper {
//  public:
//   StorageWrapper(const storage::Config &config) {
//     if (utils::DirExists(config.disk.main_storage_directory)) {
//       storage_ = new storage::DiskStorage(config);
//     } else {
//       storage_ = new storage::InMemoryStorage(config);
//     }
//   }
//   ~StorageWrapper() { delete storage_; }
//   StorageWrapper(const StorageWrapper &) = delete;
//   StorageWrapper &operator=(const StorageWrapper &) = delete;
//   StorageWrapper(StorageWrapper &&) noexcept = delete;
//   StorageWrapper &operator=(StorageWrapper &&) noexcept = delete;

//   storage::Storage *operator->() const { return storage_; }

//  private:
//   storage::Storage *storage_;
// };

/**
 * @brief Multi-database storage handler
 *
 */

class Database {
 public:
  explicit Database(const storage::Config &config)
      : trigger_store_(config.durability.storage_directory / "triggers"),
        streams_{config.durability.storage_directory / "streams"} {
    if (config.force_on_disk || utils::DirExists(config.disk.main_storage_directory)) {
      storage_ = std::make_unique<storage::DiskStorage>(config);
    } else {
      storage_ = std::make_unique<storage::InMemoryStorage>(config);
    }
  }

  /* NOTE
   * The Database object is shared. All the undelying function calls should be protected.
   * Storage function calls should already be protected; add protection where needed.
   *
   * What is not protected is the pointer storage_. This can change when switching from
   * inmemory to ondisk. This was previously protected by locking the interpreters and
   * checking if we were the only ones using the storage. This won't be possible
   * (or will be expensive) to do.
   *
   * Current implementation uses a handler of Database objects. It owns them and gives
   * shared pointers to it. These shared pointers guarantee that the object won't be
   * destroyed unless no one is using it.
   *
   * Do we add a RWLock here and protect the storage?
   * This will be difficult since a lot of the API uses a raw pointer to storage.
   * We could modify the reference counting (done via the shared_ptr) to something
   * better and when changing storage go through the handler. There we can guarantee
   * that we are the only ones using it.
   * There will be a problem of streams and triggers that rely on the undelying storage.
   * Make sure they are using the Database and not the storage pointer?
   */

  // void UpgradeToOnDisk()...
  storage::Storage *storage() { return storage_.get(); }  // This is kinda hot
  std::unique_ptr<storage::Storage::Accessor> Access(
      std::optional<storage::IsolationLevel> override_isolation_level = {}) {
    return storage_->Access(override_isolation_level);
  }
  const std::string &id() const { return storage_->id(); }
  storage::StorageMode GetStorageMode() const { return storage_->GetStorageMode(); }
  storage::StorageInfo GetInfo() const { return storage_->GetInfo(); }

  query::TriggerStore *trigger_store() { return &trigger_store_; }

  query::stream::Streams *streams() { return &streams_; }

  utils::ThreadPool *thread_pool() { return &after_commit_trigger_pool_; }
  void AddTask(std::function<void()> new_task) { after_commit_trigger_pool_.AddTask(new_task); }

 private:
  std::unique_ptr<storage::Storage> storage_;
  query::TriggerStore trigger_store_;
  utils::ThreadPool after_commit_trigger_pool_{1};
  query::stream::Streams streams_;
};

class DatabaseHandler : public Handler<Database, storage::Config> {
 public:
  using HandlerT = Handler<Database, storage::Config>;

  /**
   * @brief Generate new storage associated with the passed name.
   *
   * @param name Name associating the new interpreter context
   * @param config Storage configuration
   * @return HandlerT::NewResult
   */
  HandlerT::NewResult New(const std::string &name, storage::Config config) {
    // Control that no one is using the same data directory
    if (std::any_of(cbegin(), cend(), [&](const auto &elem) {
          return elem.second.config().durability.storage_directory == config.durability.storage_directory;
        })) {
      spdlog::info("Tried to generate new storage using a claimed directory.");
      return NewError::EXISTS;
    }
    config.name = name;  // Set storage id via config
    return HandlerT::New(name, std::forward_as_tuple(config), std::forward_as_tuple(config));
  }

  /**
   * @brief All currently active storage.
   *
   * @return std::vector<std::string>
   */
  std::vector<std::string> All() const {
    std::vector<std::string> res;
    res.reserve(std::distance(cbegin(), cend()));
    std::for_each(cbegin(), cend(), [&](const auto &elem) { res.push_back(elem.first); });
    return res;
  }
};

}  // namespace memgraph::dbms

#endif
