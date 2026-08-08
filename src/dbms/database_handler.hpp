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
#include <atomic>
#include <functional>
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
        // utils::drain_bypass: at process shutdown a tenant can still be mid-drain (draining_ ==
        // true, value_ still HOT, plain access() would refuse). It must not be skipped here --
        // this loop's job is to proactively StopAllBackgroundTasks() on every live tenant before
        // the map (and thus every Gatekeeper in it) is destroyed, and a draining tenant is no
        // exception. Calling it on a tenant whose drop already stopped its tasks is a harmless
        // no-op: Scheduler::Stop() / ThreadPool::ShutDown() / Streams::Shutdown() all guard on
        // their own stop/empty state, so a second call is a no-op rather than a double-join.
        if (auto db_acc = db.second.access(utils::drain_bypass)) {
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
  // GKInternals<Database>* isn't known when this closure is built -- the Database (and the closure
  // inside it) is constructed *inside* GKInternals, before GKInternals has a stable address (New()'s
  // map node, BuildDetached()'s Gatekeeper). `cell` is published once that address exists. Must be
  // atomic: a background thread (e.g. TTL, spawned from inside the Storage ctor during WAL replay)
  // may call the factory before publish.
  using ProtectorCell = std::shared_ptr<std::atomic<utils::GKInternals<Database> *>>;

  struct ProtectorFactoryHandle {
    std::function<storage::DatabaseProtectorPtr()> factory;
    ProtectorCell cell;
  };

  /// Builds a {factory, cell} pair. The factory mints a DatabaseProtector for whichever tenant's
  /// GKInternals is later published into `cell`; used by both New() and BuildDetached() so the
  /// factory logic lives in exactly one place.
  ///
  /// DRAIN GUARANTEE: the closure's only route to a tenant is `utils::Gatekeeper<Database>::access_via`
  /// -- the plain, drain-gated mint, never utils::drain_bypass -- and it shares its `mint_locked`
  /// predicate with both Gatekeeper::access() overloads, so a draining tenant is refused here exactly
  /// as it would be through Get(). The closure holds no DatabaseHandler pointer, no name, and no
  /// tenant registry -- only `cell` -- so it has no other route to a tenant; that structural absence
  /// is what makes the guarantee hold by construction. Its two consumers --
  /// storage::ttl::TTL (src/storage/v2/ttl.cpp) and the async indexer (src/storage/v2/async_indexer.cpp),
  /// both via Storage::make_database_protector() -- check the result for null and return/stop rather
  /// than commit. So no new DatabaseProtector can be armed for a tenant being dropped: a re-armed one
  /// would hold a live DatabaseAccess and keep the tenant's accessor count above zero indefinitely,
  /// which would stop the drain from ever converging. A null `cell` (pre-publish) fails the same way
  /// and is not a special case: it matches the pre-existing behaviour of looking up a not-yet-inserted
  /// tenant.
  ///
  /// Why the pre-publish window is safe rather than merely fail-closed: TTL retries next tick on a
  /// null protector, but the async indexer's worker thread returns from its lambda and exits for
  /// good (src/storage/v2/async_indexer.cpp) -- so a null protector must never actually reach it here.
  /// It can't: InMemoryStorage's ctor installs a deny-everything ttl_.SetUserCheck BEFORE RecoverData
  /// (src/storage/v2/inmemory/storage.cpp), and ttl_job returns on that check before it can reach this
  /// factory (src/storage/v2/ttl.cpp), so TTL can't call it during recovery, and the async indexer's
  /// queue is only fed from TTL or a live commit, neither of which runs during that window either. If
  /// a future Enqueue call site bypasses that user check, this window stops being safe.
  ProtectorFactoryHandle MakeDatabaseProtectorFactory() {
    auto cell = std::make_shared<std::atomic<utils::GKInternals<Database> *>>(nullptr);
    auto factory = [cell]() -> storage::DatabaseProtectorPtr {
      auto *internals = cell->load(std::memory_order_acquire);
      if (internals == nullptr) return nullptr;
      if (auto db_acc = utils::Gatekeeper<Database>::access_via(internals)) {
        return std::make_unique<DatabaseProtector>(*db_acc);
      }
      return nullptr;
    };
    return {std::move(factory), std::move(cell)};
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
          //
          // utils::drain_bypass: a draining tenant has not been destroyed yet and still owns its
          // storage directory, so it must stay visible to this collision check. Safe unlike a
          // bypassed mint on a path that can win Accessor::try_delete() (see Handler<T>::TryDelete's
          // comment) -- this scan only reads config(), it never destroys anything.
          auto db_acc = elem.second.access(utils::drain_bypass);
          if (!db_acc) return false;
          return db_acc->get()->config().durability.storage_directory == config.durability.storage_directory;
        })) {
      spdlog::info("Tried to generate new storage using a claimed directory.");
      return std::unexpected{NewError::EXISTS};
    }

    auto handle = MakeDatabaseProtectorFactory();
    auto result =
        HandlerT::New(std::piecewise_construct, *config.salient.name.str_view(), config, std::move(handle.factory));
    if (result.has_value()) {
      // Caller (DbmsHandler::New_) holds lock_ exclusive, so items_ cannot have moved this node yet.
      auto *gk = GetGatekeeper(*config.salient.name.str_view());
      MG_ASSERT(gk,
                "Database {} not found immediately after HandlerT::New emplaced it under lock_.",
                *config.salient.name.str_view());
      handle.cell->store(gk->internals(), std::memory_order_release);
    }
    return result;
  }

  /**
   * @brief Build a Database gatekeeper OFF the map (no insert), recovering it if the config asks for
   *        it. Used by the hot/cold resume engine: the winner rebuilds the storage on its own thread
   *        without holding the handler under lock, then move-assigns the returned (HOT) gatekeeper
   *        over the in-map COLD shell. The database-protector factory is published against this
   *        Gatekeeper's own GKInternals before it is returned, so it stays correct across that later
   *        move-assignment (a unique_ptr pointer transfer -- see utils::Gatekeeper<T>::internals()).
   *
   * @param config Storage configuration (already path-resolved by the caller)
   * @return a HOT utils::Gatekeeper<Database> by value (move)
   */
  utils::Gatekeeper<Database> BuildDetached(storage::Config config) {
    auto handle = MakeDatabaseProtectorFactory();
    // Build OFF the map (no insert). The Database ctor recovers when
    // config.durability.recover_on_startup == true.
    utils::Gatekeeper<Database> gk{std::move(config), std::move(handle.factory)};
    handle.cell->store(gk.internals(), std::memory_order_release);
    return gk;
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
   * Deliberately stays gated on the plain, drain-gated Get() -- NOT utils::drain_bypass -- even
   * though that looks like an omission next to New()'s collision scan above. A draining tenant's
   * config being unreachable by name here is intentional: every remaining caller either only cares
   * about a HOT tenant (DbmsHandler::GetHotUuid, StorageDir_/SetupDefault_ on the always-HOT default
   * db) or is a drop-family entry point for which "config not found" is meant to mean "give up on
   * this name" -- DbmsHandler::TryDelete additionally special-cases an already-draining tenant with
   * its own is_draining() check *before* reaching this lookup, so it reports the accurate USING
   * rather than relying on (or being confused by) this refusal. DbmsHandler::Delete_, the drop path
   * itself, never calls this at all: once it holds the tenant's drain_bypass accessor it reads the
   * storage directory straight off `database->config()`, so this gate cannot block its own drop.
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
