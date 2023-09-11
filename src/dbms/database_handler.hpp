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

#include "dbms/database.hpp"

#include "handler.hpp"

namespace memgraph::dbms {

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

/**
 * @brief Multi-database storage handler
 *
 */
class DatabaseHandler : public Handler<Database> {
 public:
  using HandlerT = Handler<Database>;

  /**
   * @brief Generate new storage associated with the passed name.
   *
   * @param name Name associating the new interpreter context
   * @param config Storage configuration
   * @return HandlerT::NewResult
   */
  HandlerT::NewResult New(std::string_view name, storage::Config config) {
    // Control that no one is using the same data directory
    if (std::any_of(begin(), end(), [&](auto &elem) {
          auto db_acc = elem.second.Access();
          return !db_acc || db_acc->get()->config().durability.storage_directory == config.durability.storage_directory;
        })) {
      spdlog::info("Tried to generate new storage using a claimed directory.");
      return NewError::EXISTS;
    }
    config.name = name;  // Set storage id via config
    return HandlerT::New(std::piecewise_construct, name, config);
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

  std::optional<storage::Config> GetConfig(std::string_view name) {
    auto db = Get(name);
    if (db) {
      return (*db)->config();
    }
    return std::nullopt;
  }
};

}  // namespace memgraph::dbms

#endif
