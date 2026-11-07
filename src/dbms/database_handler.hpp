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
#include "dbms/dbms_name.hpp"

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
class DatabaseHandler : private Handler<Database> {
 public:
  using HandlerT = Handler<Database>;

  // The base is private so its unchecked EmplaceColdShell/Rename stay unreachable from outside; re-export
  // exactly what callers outside the class use.
  using HandlerT::begin;
  using HandlerT::cbegin;
  using HandlerT::cend;
  using HandlerT::ClearDrainHook;
  using HandlerT::DeferDelete;
  using HandlerT::end;
  using HandlerT::EraseColdShell;
  using HandlerT::Get;
  using HandlerT::GetGatekeeper;
  using HandlerT::Has;
  using HandlerT::IsPending;
  using HandlerT::PendingItems;
  using HandlerT::SetDrainHook;
  using HandlerT::size;
  using HandlerT::TryDelete;

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

 public:
  /**
   * @brief Generate new storage associated with the passed name.
   *
   * @param name Name associating the new interpreter context
   * @param config Storage configuration
   * @return HandlerT::NewResult
   */
  HandlerT::NewResult New(storage::Config config) {
    if (!DatabaseNameIsWellFormed(*config.salient.name.str_view())) {
      return std::unexpected{NewError::INVALID_NAME};
    }
    return DoNew_(std::move(config));
  }

  // Shadows Handler::Rename: gate the locally minted target before any mutation.
  std::expected<void, RenameError> Rename(std::string_view old_name, std::string_view new_name) {
    if (!DatabaseNameIsWellFormed(new_name)) {
      return std::unexpected{RenameError::INVALID_NAME};
    }
    return HandlerT::Rename(old_name, new_name);
  }

  // Adopt entry for a name from durable state or MAIN replication: warn, proceed, never abort.
  HandlerT::NewResult AdoptLegacy(storage::Config config) {
    WarnIfInvalid_(*config.salient.name.str_view(), "durable state or MAIN replication");
    return DoNew_(std::move(config));
  }

  // Adopt entry for a legacy name as a COLD shell (boot cold restore): warn, proceed, stay resumable.
  utils::Gatekeeper<Database> *AdoptColdShell(std::string_view name) {
    WarnIfInvalid_(name, "cold restore from durable state");
    return HandlerT::EmplaceColdShell(name);
  }

  // Adopt entry for a replica rename target from MAIN: warn, proceed so the replica converges.
  std::expected<void, RenameError> AdoptReplicatedRename(std::string_view old_name, std::string_view new_name) {
    WarnIfInvalid_(new_name, "replica rename from MAIN");
    return HandlerT::Rename(old_name, new_name);
  }

  // Adopt entry for a keep-old rollback destination: warn, proceed, the rollback never fails on the name.
  std::expected<void, RenameError> AdoptRenameRollback(std::string_view old_name, std::string_view new_name) {
    WarnIfInvalid_(new_name, "rename rollback");
    return HandlerT::Rename(old_name, new_name);
  }

 private:
  // Legacy names are adopted, never rejected: warn loudly but keep the tenant alive.
  static void WarnIfInvalid_(std::string_view name, std::string_view origin) {
    if (!DatabaseNameIsWellFormed(name)) {
      spdlog::warn(
          "Adopting database name {:?} ({}), which does not satisfy the database name rule. The tenant stays "
          "addressable; RENAME it to a well-formed name.",
          name,
          origin);
    }
  }

  // The one registry insert body shared by the strict and the adopt entries.
  HandlerT::NewResult DoNew_(storage::Config config) {
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

    return HandlerT::New(std::piecewise_construct, *config.salient.name.str_view(), config);
  }

 public:
  /**
   * @brief Build a Database gatekeeper OFF the map (no insert), recovering it if the config asks for
   *        it. Used by the hot/cold resume engine: the winner rebuilds the storage on its own thread
   *        without holding the handler under lock, then move-assigns the returned (HOT) gatekeeper
   *        over the in-map COLD shell. The Database receives a handle to this gatekeeper's internals,
   *        which survive that move-assign, so its background workers keep a valid protector source.
   *
   * @param config Storage configuration (already path-resolved by the caller)
   * @return a HOT utils::Gatekeeper<Database> by value (move)
   */
  utils::Gatekeeper<Database> BuildDetached(storage::Config config) {
    // Build OFF the map (no insert). The Database ctor recovers when
    // config.durability.recover_on_startup == true. Returned by value (move).
    return utils::Gatekeeper<Database>{std::move(config)};
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
