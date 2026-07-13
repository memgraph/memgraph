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

#ifdef MG_ENTERPRISE

#include <algorithm>
#include <iterator>
#include <memory>
#include <optional>
#include <string_view>

#include "dbms/database.hpp"
#include "dbms/database_protector.hpp"

#include "handler.hpp"

namespace memgraph::dbms {

/* NOTE
 * The Database object is shared. All the higher-level function calls should be protected.
 * Storage function calls should already be protected; add protection where needed.
 *
 * Current implementation uses a handler of Database objects. It owns them and gives
 * Gatekeeper::Accessor to it. These guarantee that the object won't be
 * destroyed unless no one is using it.
 */

/**Config
 * @brief Multi-database storage handler
 *
 */
class DatabaseHandler : public Handler<Database> {
 public:
  using HandlerT = Handler<Database>;

  DatabaseHandler() = default;

  ~DatabaseHandler() override {
    for (auto &db : *this) {
      try {
        if (auto db_acc = db.second.access()) {
          (*db_acc)->StopAllBackgroundTasks();
        }
      } catch (std::exception const &e) {
        spdlog::error("Exception in DatabaseHandler destructor: {}", e.what());
      } catch (...) {
        spdlog::error("Unknown exception in DatabaseHandler destructor");
      }
    }
  }

 private:
  /// Returns a factory callable that produces a DatabaseProtector for the database named
  /// @p db_name by looking it up in this handler at call time. Used by both New_() and
  /// BuildDetached() so the factory logic lives in exactly one place.
  auto MakeDatabaseProtectorFactory(std::string db_name) {
    return [this, db_name = std::move(db_name)]() -> storage::DatabaseProtectorPtr {
      if (auto db_gatekeeper_opt = this->Get(db_name)) {
        return std::make_unique<DatabaseProtector>(*db_gatekeeper_opt);
      }
      // Fallback: return null if database not found (shouldn't happen in normal operation)
      return nullptr;
    };
  }

 public:
  /**
   * @brief Generate new storage associated with the passed name.
   *
   * @param name Name associating the new interpreter context
   * @param config Storage configuration
   * @return HandlerT::NewResult
   */
  HandlerT::NewResult New(storage::Config config) {
    // Control that no one is using the same data directory
    if (std::ranges::any_of(*this, [&](auto &elem) {
          // A hot/cold COLD shell is a no-value gatekeeper (access() == nullopt). It does not hold a
          // live storage claiming a directory (its durable dir is unique, UUID-derived, and a resume
          // rebuilds via BuildDetached, never New()), so it cannot collide — skip it. MG_ASSERTing
          // has_value() here would abort whenever New() runs with any tenant suspended (e.g. the
          // replica reconcile materializing an absent COLD tenant, or a plain CREATE DATABASE).
          auto db_acc = elem.second.access();
          if (!db_acc) return false;
          return db_acc->get()->config().durability.storage_directory == config.durability.storage_directory;
        })) {
      spdlog::info("Tried to generate new storage using a claimed directory.");
      return std::unexpected{NewError::EXISTS};
    }

    return HandlerT::New(std::piecewise_construct,
                         *config.salient.name.str_view(),
                         config,
                         MakeDatabaseProtectorFactory(config.salient.name.str()));
  }

  /**
   * @brief Build a Database gatekeeper OFF the map (no insert), recovering it if the config asks for
   *        it. Used by the hot/cold resume engine: the winner rebuilds the storage on its own thread
   *        without holding the handler under lock, then move-assigns the returned (HOT) gatekeeper
   *        over the in-map COLD shell. The database-protector factory resolves the gatekeeper by name
   *        via this->Get(db_name); after the caller publishes it at `name`, that lookup finds the
   *        (now HOT) in-map gatekeeper.
   *
   * @param config Storage configuration (already path-resolved by the caller)
   * @return a HOT utils::Gatekeeper<Database> by value (move)
   */
  utils::Gatekeeper<Database> BuildDetached(storage::Config config) {
    // Snap name before the move so MakeDatabaseProtectorFactory doesn't read moved-from config.
    auto factory = MakeDatabaseProtectorFactory(config.salient.name.str());
    // Build OFF the map (no insert). The Database ctor recovers when
    // config.durability.recover_on_startup == true. Returned by value (move).
    return utils::Gatekeeper<Database>{std::move(config), std::move(factory)};
  }

  /**
   * @brief All currently active storage.
   *
   * @return std::vector<std::string>
   */
  std::vector<std::string> All() const {
    std::vector<std::string> res;
    res.reserve(std::distance(cbegin(), cend()));
    std::ranges::for_each(*this, [&](const auto &elem) {
      const auto is_marked_for_deletion = elem.second.is_marked_for_deletion();
      if (is_marked_for_deletion.has_value() && !is_marked_for_deletion.value()) res.push_back(elem.first);
    });
    return res;
  }

  /**
   * @brief Get the associated storage's configuration
   *
   * @param name
   * @return std::optional<storage::Config>
   */
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
