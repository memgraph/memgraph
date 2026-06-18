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

#include "dbms/replication_handlers.hpp"

#include <chrono>
#include <mutex>
#include <thread>

#include "dbms/dbms_handler.hpp"
#include "dbms/inmemory/replication_handlers.hpp"
#include "dbms/rpc.hpp"
#include "flags/experimental.hpp"  // MED1: warn when following MAIN's hot/cold with the flag off locally
#include "license/license.hpp"
#include "system/state.hpp"

#include "rpc/utils.hpp"

namespace memgraph::rpc {
class FileReplicationHandler;
}  // namespace memgraph::rpc

namespace memgraph::dbms {

#ifdef MG_ENTERPRISE

namespace {
// MED1 (C15): a replica applies MAIN's hot/cold SUSPEND/RESUME RPCs unconditionally (replicas mirror
// MAIN's authoritative {HOT ∪ COLD} set; gating the apply on the local flag would break convergence).
// But following a feature whose experiment flag is OFF locally is almost certainly a misconfiguration,
// so surface it once. The flag must be set consistently across the cluster.
void WarnHotColdFlagMismatchOnce() {
  if (flags::AreExperimentsEnabled(flags::Experiments::HOT_COLD_DATABASES)) return;
  static std::once_flag warned;
  std::call_once(warned, [] {
    spdlog::warn(
        "Received a hot/cold SUSPEND/RESUME replication message from MAIN, but the hot-cold-databases "
        "experiment is DISABLED on this instance. Following MAIN's hot/cold state regardless (a replica "
        "mirrors MAIN). Set --experimental-enabled=hot-cold-databases consistently across the cluster.");
  });
}
}  // namespace

void CreateDatabaseHandler(system::ReplicaHandlerAccessToState &system_state_access,
                           const std::optional<utils::UUID> &current_main_uuid, DbmsHandler &dbms_handler,
                           uint64_t const request_version, slk::Reader *req_reader, slk::Builder *res_builder) {
  using storage::replication::CreateDatabaseRes;
  CreateDatabaseRes res(CreateDatabaseRes::Result::FAILURE);

  // Ignore if no license
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    spdlog::error(
        "Handling CreateDatabase, an enterprise RPC message, without license. Check your license status by running "
        "SHOW LICENSE INFO.");
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  storage::replication::CreateDatabaseReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  if (current_main_uuid != req.main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::CreateDatabaseReq::kType.name);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  // Note: No need to check epoch, recovery mechanism is done by a full uptodate snapshot
  //       of the set of databases. Hence, no history exists to maintain regarding epoch change.
  //       If MAIN has changed we need to check this new group_timestamp is consistent with
  //       what we have so far.

  if (req.expected_group_timestamp != system_state_access.LastCommitedTS()) {
    spdlog::debug("CreateDatabaseHandler: bad expected timestamp {},{}",
                  req.expected_group_timestamp,
                  system_state_access.LastCommitedTS());
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  try {
    // Create new
    if (auto const new_db = dbms_handler.Update(req.config); new_db.has_value()) {
      // Successfully create db
      res = CreateDatabaseRes(CreateDatabaseRes::Result::SUCCESS);
      spdlog::debug("CreateDatabaseHandler: SUCCESS");
    }
  } catch (...) {
    // Failure
  }

  rpc::SendFinalResponse(res, request_version, res_builder);
}

void DropDatabaseHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access,
                         const std::optional<utils::UUID> &current_main_uuid, DbmsHandler &dbms_handler,
                         uint64_t const request_version, slk::Reader *req_reader, slk::Builder *res_builder) {
  using memgraph::storage::replication::DropDatabaseRes;
  DropDatabaseRes res(DropDatabaseRes::Result::FAILURE);

  // Ignore if no license
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    spdlog::error(
        "Handling DropDatabase, an enterprise RPC message, without license. Check your license status by running SHOW "
        "LICENSE INFO.");
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  memgraph::storage::replication::DropDatabaseReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  if (current_main_uuid != req.main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, memgraph::storage::replication::DropDatabaseReq::kType.name);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  // Note: No need to check epoch, recovery mechanism is done by a full uptodate snapshot
  //       of the set of databases. Hence no history exists to maintain regarding epoch change.
  //       If MAIN has changed we need to check this new group_timestamp is consistent with
  //       what we have so far.

  if (req.expected_group_timestamp != system_state_access.LastCommitedTS()) {
    spdlog::debug("DropDatabaseHandler: bad expected timestamp {},{}",
                  req.expected_group_timestamp,
                  system_state_access.LastCommitedTS());
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  try {
    // NOTE: Single communication channel can exist at a time, no other database can be deleted/created at the moment.
    auto new_db = dbms_handler.Delete(req.uuid);
    if (!new_db) {
      if (new_db.error() == DeleteError::NON_EXISTENT) {
        // Nothing to drop
        res = DropDatabaseRes(DropDatabaseRes::Result::NO_NEED);
      }
    } else {
      // Successfully drop db
      res = DropDatabaseRes(DropDatabaseRes::Result::SUCCESS);
      spdlog::debug("DropDatabaseHandler: SUCCESS");
    }
  } catch (...) {
    // Failure
  }

  rpc::SendFinalResponse(res, request_version, res_builder);
}

void RenameDatabaseHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access,
                           const std::optional<utils::UUID> &current_main_uuid, DbmsHandler &dbms_handler,
                           uint64_t const request_version, slk::Reader *req_reader, slk::Builder *res_builder) {
  using memgraph::storage::replication::RenameDatabaseRes;
  RenameDatabaseRes res(RenameDatabaseRes::Result::FAILURE);

  // Ignore if no license
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    spdlog::error(
        "Handling RenameDatabase, an enterprise RPC message, without license. Check your license status by running "
        "SHOW "
        "LICENSE INFO.");
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  memgraph::storage::replication::RenameDatabaseReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  if (current_main_uuid != req.main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, memgraph::storage::replication::RenameDatabaseReq::kType.name);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  // Note: No need to check epoch, recovery mechanism is done by a full uptodate snapshot
  //       of the set of databases. Hence no history exists to maintain regarding epoch change.
  //       If MAIN has changed we need to check this new group_timestamp is consistent with
  //       what we have so far.

  if (req.expected_group_timestamp != system_state_access.LastCommitedTS()) {
    spdlog::debug("RenameDatabaseHandler: bad expected timestamp {},{}",
                  req.expected_group_timestamp,
                  system_state_access.LastCommitedTS());
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  try {
    // NOTE: Single communication channel can exist at a time, no other database can be renamed/created/deleted at the
    // moment.
    auto rename_result = dbms_handler.Rename(req.old_name, req.new_name);
    if (!rename_result) {
      if (rename_result.error() == RenameError::NON_EXISTENT) {
        // Nothing to rename
        system_state_access.SetLastCommitedTS(req.new_group_timestamp);
        res = RenameDatabaseRes(RenameDatabaseRes::Result::NO_NEED);
      }
    } else {
      // Successfully renamed db
      system_state_access.SetLastCommitedTS(req.new_group_timestamp);
      res = RenameDatabaseRes(RenameDatabaseRes::Result::SUCCESS);
      spdlog::debug("RenameDatabaseHandler: SUCCESS updated LCTS to {}", req.new_group_timestamp);
    }
  } catch (...) {
    // Failure
    spdlog::trace(R"(RenameDatabaseHandler: Failed to rename database "{}" to "{}".)", req.old_name, req.new_name);
  }

  rpc::SendFinalResponse(res, request_version, res_builder);
}

void SuspendDatabaseHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access,
                            const std::optional<utils::UUID> &current_main_uuid, DbmsHandler &dbms_handler,
                            uint64_t const request_version, slk::Reader *req_reader, slk::Builder *res_builder) {
  using memgraph::storage::replication::SuspendDatabaseRes;
  SuspendDatabaseRes res(SuspendDatabaseRes::Result::FAILURE);

  // Ignore if no license
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    spdlog::error(
        "Handling SuspendDatabase, an enterprise RPC message, without license. Check your license status by running "
        "SHOW LICENSE INFO.");
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  memgraph::storage::replication::SuspendDatabaseReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  if (current_main_uuid != req.main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, memgraph::storage::replication::SuspendDatabaseReq::kType.name);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  if (req.expected_group_timestamp != system_state_access.LastCommitedTS()) {
    spdlog::debug("SuspendDatabaseHandler: bad expected timestamp {},{}",
                  req.expected_group_timestamp,
                  system_state_access.LastCommitedTS());
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  // MED1: warn (once) only when we are actually about to APPLY MAIN's suspend — past the main-uuid and
  // timestamp guards — so a wrong-main / stale-timestamp RPC does not consume the warn slot spuriously.
  WarnHotColdFlagMismatchOnce();

  try {
    // TD-3': drop any cached 2PC accessor belonging to THIS tenant before teardown. The accessor is
    // storage-level (not gatekeeper-counted), so the suspend freeze would not drain it, and tearing
    // down the storage with it still cached would dangle it.
    InMemoryReplicationHandlers::AbortTwoPCForTenant(req.uuid);

    // SY-1: no repl_state lock held (we follow the DropDatabase pattern). SY-2: SuspendByUUID ->
    // Suspend_ uses the gatekeeper's bounded count==1 drain. On a replica IsReplicationParticipant()
    // is false so no guard bypass is needed.
    auto result = dbms_handler.SuspendByUUID(req.uuid);
    if (result) {
      res = SuspendDatabaseRes(SuspendDatabaseRes::Result::SUCCESS);
      spdlog::debug("SuspendDatabaseHandler: SUCCESS");
    } else if (result.error() == DbmsHandler::SuspendError::NON_EXISTENT && dbms_handler.IsKnownTenant(req.uuid)) {
      // Holistic-review #3: NON_EXISTENT means "not HOT". Only treat it as an idempotent NO_NEED when the
      // tenant is KNOWN (already COLD) — i.e. genuinely already in the target state. If the tenant is
      // unknown entirely, this is a divergence (MAIN suspended a tenant this replica is missing); leave
      // the apply FAILURE so the replica latches BEHIND and SystemRecovery (C8) supplies it.
      res = SuspendDatabaseRes(SuspendDatabaseRes::Result::NO_NEED);
    }
    // Any other error (e.g. ACTIVE_CONNECTIONS bounded-drain timeout) leaves FAILURE. SY-2: the
    // replica does NOT silently diverge — the system timestamp still advances via FinalizeSystemTxRpc
    // and the failed apply flags this replica BEHIND, so it reconciles on the next ordered re-sync /
    // SystemRecovery (C8).
  } catch (const std::exception &e) {
    // FAILURE: log so a diverged replica (SY-2) has a diagnostic anchor.
    spdlog::error(
        "SuspendDatabaseHandler: failed to apply suspend for database {}: {}", std::string{req.uuid}, e.what());
  } catch (...) {
    spdlog::error("SuspendDatabaseHandler: failed to apply suspend for database {}", std::string{req.uuid});
  }

  rpc::SendFinalResponse(res, request_version, res_builder);
}

void ResumeDatabaseHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access,
                           const std::optional<utils::UUID> &current_main_uuid, DbmsHandler &dbms_handler,
                           uint64_t const request_version, slk::Reader *req_reader, slk::Builder *res_builder) {
  using memgraph::storage::replication::ResumeDatabaseRes;
  ResumeDatabaseRes res(ResumeDatabaseRes::Result::FAILURE);

  // Ignore if no license
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    spdlog::error(
        "Handling ResumeDatabase, an enterprise RPC message, without license. Check your license status by running "
        "SHOW LICENSE INFO.");
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  memgraph::storage::replication::ResumeDatabaseReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  if (current_main_uuid != req.main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, memgraph::storage::replication::ResumeDatabaseReq::kType.name);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  if (req.expected_group_timestamp != system_state_access.LastCommitedTS()) {
    spdlog::debug("ResumeDatabaseHandler: bad expected timestamp {},{}",
                  req.expected_group_timestamp,
                  system_state_access.LastCommitedTS());
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  // MED1: warn (once) only when actually applying MAIN's resume — past the main-uuid/timestamp guards.
  WarnHotColdFlagMismatchOnce();

  try {
    // SY-1: ResumeByUUID -> Resume_(rewire_replication=false). The apply thread holds no repl_state
    // lock, so the post-publish replication arm (which would take a repl_state write lock) is skipped.
    auto result = dbms_handler.ResumeByUUID(req.uuid);
    if (result) {
      res = ResumeDatabaseRes(ResumeDatabaseRes::Result::SUCCESS);
      spdlog::debug("ResumeDatabaseHandler: SUCCESS");
    } else if (result.error() == DbmsHandler::ResumeError::NON_EXISTENT && dbms_handler.IsKnownTenant(req.uuid)) {
      // Holistic-review #3: NON_EXISTENT means "not in the suspended-set". Only treat it as an idempotent
      // NO_NEED when the tenant is KNOWN (already HOT here). If it is unknown entirely, this is a
      // divergence (MAIN resumed a tenant this replica is missing); leave the apply FAILURE so the replica
      // latches BEHIND and SystemRecovery (C8) supplies it.
      res = ResumeDatabaseRes(ResumeDatabaseRes::Result::NO_NEED);
    }
    // RECOVERY_FAILED leaves FAILURE -> diverge-then-reconcile via SystemRecovery (C8).
  } catch (const std::exception &e) {
    // FAILURE: log so a diverged replica (SY-2) has a diagnostic anchor.
    spdlog::error("ResumeDatabaseHandler: failed to apply resume for database {}: {}", std::string{req.uuid}, e.what());
  } catch (...) {
    spdlog::error("ResumeDatabaseHandler: failed to apply resume for database {}", std::string{req.uuid});
  }

  rpc::SendFinalResponse(res, request_version, res_builder);
}

namespace {
// SR-1′(2): force-suspend a tenant on the replica during recovery. On a replica the only transient
// suspend failure is ACTIVE_CONNECTIONS (a background accessor — GC/periodic-snapshot — still live;
// no client write conns exist, and a data-delta accessor cannot be in flight because the same
// single-threaded RPC server is currently running this recovery). The other SuspendError reasons are
// structurally unreachable here (DEFAULT_DB/NOT_IN_MEMORY/DURABILITY_INCOMPLETE are config-fixed, and
// there is no MAIN-side replication-participant gate). The engine's try_begin_suspend() already waits
// 100ms for count==1; this bounded outer retry covers a slow background accessor. On timeout the caller flags
// BEHIND and the whole recovery is retried (SY-2 diverge-then-reconcile).
bool ForceSuspendForRecovery(DbmsHandler &dbms_handler, std::string_view name) {
  constexpr auto kRetryStep = std::chrono::milliseconds(20);
  // Bounded so it does not hold the single-threaded replica RPC server (heartbeat/WAL deltas) for
  // long: the engine's try_begin_suspend() already waits 100ms/attempt, the accessor being drained is
  // a background GC/snapshot one (drains in well under a second), and the outer BEHIND->reconcile loop
  // retries the whole recovery anyway, so a short cap costs at most one extra cheap retry.
  constexpr auto kTimeout = std::chrono::seconds(2);
  const auto deadline = std::chrono::steady_clock::now() + kTimeout;
  while (true) {
    // HOLE-1 (C15): recovery bypasses the durability-complete gate (the tenant is already COLD on
    // MAIN; the consolidating snapshot is written unconditionally and read back on resume). Without
    // the bypass a replica whose durability config differs from MAIN's would fail here with
    // DURABILITY_INCOMPLETE and spin forever in the BEHIND->reconcile loop.
    auto res = dbms_handler.SuspendForRecovery(name);
    if (res.has_value()) return true;
    if (res.error() != DbmsHandler::SuspendError::ACTIVE_CONNECTIONS) {
      spdlog::debug("SystemRecoveryHandler: suspend of \"{}\" failed (non-transient).", name);
      return false;
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      spdlog::debug("SystemRecoveryHandler: suspend of \"{}\" timed out draining accessors.", name);
      return false;
    }
    std::this_thread::sleep_for(kRetryStep);
  }
}
}  // namespace

bool SystemRecoveryHandler(DbmsHandler &dbms_handler, const std::vector<storage::SalientConfig> &database_configs,
                           const std::vector<storage::ColdTenantRecovery> &cold_databases) {
  /*
   * NO LICENSE
   */
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    spdlog::error(
        "Handling SystemRecovery, an enterprise RPC message, without license. Check your license status by running "
        "SHOW LICENSE INFO.");
    for (const auto &config : database_configs) {
      // Only handle default DB
      if (config.name != kDefaultDB) continue;
      try {
        if (!dbms_handler.Update(config)) {
          return false;
        }
      } catch (const UnknownDatabaseException &) {
        return false;
      }
    }
    return true;
  }

  /*
   * MULTI-TENANCY
   */
  // C16 (MED2): the COLD set arrives as one ColdTenantRecovery per tenant (salient + stats + epoch),
  // so the salient<->stats pairing is structural — the earlier mismatched-length guard is gone.

  // H4: seed the leftover-delete set from HOT *and* COLD tenants. All() skips COLD shells, so a
  // tenant MAIN has dropped while this replica holds it COLD would otherwise be in neither the
  // incoming sets nor `old` and would never be reconciled away (permanent divergence). The DROP of
  // a COLD tenant is handled cold-aware by Delete() (see DeleteCold_). AllWithHotColdStatus() is
  // de-duplicated (a SUSPENDING-transient tenant is listed once, as COLD).
  std::vector<std::string> old;
  {
    auto hot_cold = dbms_handler.AllWithHotColdStatus();
    old.reserve(hot_cold.size());
    for (auto &[name, _status] : hot_cold) old.emplace_back(std::move(name));
  }

  // Check/create the incoming HOT dbs.
  for (const auto &config : database_configs) {
    const auto name = std::string{*config.name.str_view()};
    // SR-1′(1): MAIN lists this name HOT but the replica holds it COLD. Update() would throw
    // UnknownDatabaseException on the COLD shell, so resume it (rewire=false) first.
    if (dbms_handler.IsSuspended(name)) {
      if (!dbms_handler.ResumeForRecovery(name).has_value()) {
        spdlog::debug("SystemRecoveryHandler: failed to resume COLD database \"{}\" to match MAIN (HOT).", name);
        return false;
      }
    }
    try {
      if (!dbms_handler.Update(config)) {
        spdlog::debug("SystemRecoveryHandler: Failed to update database \"{}\".", name);
        return false;
      }
    } catch (const UnknownDatabaseException &) {
      spdlog::debug("SystemRecoveryHandler: UnknownDatabaseException");
      return false;
    }
    std::erase(old, name);
  }

  // Reconcile the incoming COLD set: each named tenant must end as a COLD shell carrying MAIN's stats
  // and epoch metadata.
  for (const auto &cold : cold_databases) {
    const auto &config = cold.salient;
    const auto name = std::string{*config.name.str_view()};
    // SR-1: exempt COLD names from the leftover-delete loop below — a replica-HOT tenant that MAIN
    // now lists COLD must be reconciled to COLD, not dropped.
    std::erase(old, name);

    if (dbms_handler.IsSuspended(name)) {
      // Already COLD here — refresh MAIN's as-of-suspend stats snapshot (R11) AND epoch metadata
      // (C16/MED2): a converged-then-promoted replica needs MAIN's epoch to record the right boundary.
      dbms_handler.ApplyColdRecoveryMeta(name, cold);
      continue;
    }

    // Currently HOT (force-suspend) or absent (create HOT first, then suspend). Update() creates a
    // missing tenant and is a no-op-ish salient refresh for an existing HOT one.
    try {
      if (!dbms_handler.Update(config)) {
        spdlog::debug("SystemRecoveryHandler: failed to materialize database \"{}\" before suspend.", name);
        return false;
      }
    } catch (const UnknownDatabaseException &) {
      spdlog::debug("SystemRecoveryHandler: UnknownDatabaseException creating COLD database \"{}\".", name);
      return false;
    }
    if (!ForceSuspendForRecovery(dbms_handler, name)) {
      spdlog::debug("SystemRecoveryHandler: failed to suspend database \"{}\" to match MAIN (COLD).", name);
      return false;
    }
    dbms_handler.ApplyColdRecoveryMeta(name, cold);
  }

  // Delete all the leftover old dbs (neither incoming HOT nor COLD).
  for (const auto &remove_db : old) {
    const auto del = dbms_handler.Delete(remove_db);
    if (!del) {
      // Some errors are not terminal
      if (del.error() == DeleteError::DEFAULT_DB || del.error() == DeleteError::NON_EXISTENT) {
        spdlog::debug("SystemRecoveryHandler: Dropped database \"{}\".", remove_db);
        continue;
      }
      spdlog::debug("SystemRecoveryHandler: Failed to drop database \"{}\".", remove_db);
      return false;
    }
  }

  /*
   * SUCCESS
   */
  return true;
}

namespace {

void TenantProfileHandler(system::ReplicaHandlerAccessToState &system_state_access,
                          const std::optional<utils::UUID> &current_main_uuid, dbms::DbmsHandler &dbms_handler,
                          uint64_t const request_version, slk::Reader *req_reader, slk::Builder *res_builder) {
  storage::replication::TenantProfileReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);
  storage::replication::TenantProfileRes res(false);

  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    spdlog::error("Handling TenantProfile RPC without enterprise license.");
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  if (current_main_uuid != req.main_uuid) {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::TenantProfileReq::kType.name);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  if (req.expected_group_timestamp != system_state_access.LastCommitedTS()) {
    spdlog::debug("TenantProfileHandler: bad expected timestamp");
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  using Action = storage::replication::TenantProfileReq::Action;
  res = storage::replication::TenantProfileRes(true);
  try {
    switch (req.action) {
      case Action::CREATE: {
        auto result = dbms_handler.CreateTenantProfile(req.profile.name, req.profile.memory_limit, /*sys_txn=*/nullptr);
        if (!result && result.error() == TenantProfiles::CreateError::DURABILITY_ERROR) {
          spdlog::error("TenantProfileHandler: CREATE for profile '{}' failed — KVStore I/O error", req.profile.name);
          res.success = false;
        }
        break;
      }
      case Action::ALTER: {
        auto result = dbms_handler.AlterTenantProfile(req.profile.name, req.profile.memory_limit, /*sys_txn=*/nullptr);
        if (!result) {
          if (result.error() == TenantProfiles::AlterError::NOT_FOUND) {
            spdlog::warn("TenantProfileHandler: ALTER for non-existent tenant profile '{}'", req.profile.name);
          } else {
            spdlog::error("TenantProfileHandler: ALTER for profile '{}' failed — KVStore I/O error", req.profile.name);
          }
          res.success = false;
        }
        break;
      }
      case Action::DROP: {
        auto result = dbms_handler.DropTenantProfile(req.profile.name, /*sys_txn=*/nullptr);
        if (!result) {
          switch (result.error()) {
            case TenantProfiles::DropError::NOT_FOUND:
              // Benign: replica may have already pruned this profile or never had it.
              break;
            case TenantProfiles::DropError::HAS_ATTACHED_DATABASES:
              spdlog::warn("TenantProfileHandler: DROP for profile '{}' rejected — has attached databases",
                           req.profile.name);
              res.success = false;
              break;
            case TenantProfiles::DropError::DURABILITY_ERROR:
              spdlog::error("TenantProfileHandler: DROP for profile '{}' failed — KVStore I/O error", req.profile.name);
              res.success = false;
              break;
          }
        }
        break;
      }
      case Action::SET_ON_DATABASE: {
        auto result = dbms_handler.SetTenantProfileOnDatabase(req.profile.name, req.db_name, /*sys_txn=*/nullptr);
        if (!result) {
          if (result.error() == TenantProfiles::AttachError::PROFILE_NOT_FOUND) {
            spdlog::warn("TenantProfileHandler: SET_ON_DATABASE for non-existent tenant profile '{}'",
                         req.profile.name);
          } else {
            spdlog::error("TenantProfileHandler: SET_ON_DATABASE for profile '{}' failed — KVStore I/O error",
                          req.profile.name);
          }
          res.success = false;
        }
        break;
      }
      case Action::REMOVE_FROM_DATABASE: {
        auto result = dbms_handler.RemoveTenantProfileFromDatabase(req.db_name, /*sys_txn=*/nullptr);
        if (!result) {
          if (result.error() == TenantProfiles::DetachError::DURABILITY_ERROR) {
            spdlog::error("TenantProfileHandler: REMOVE_FROM_DATABASE for db '{}' failed — KVStore I/O error",
                          req.db_name);
            res.success = false;
          } else {
            spdlog::warn("TenantProfileHandler: REMOVE_FROM_DATABASE — db '{}' not attached to any profile on replica",
                         req.db_name);
          }
        }
        break;
      }
      default:
        spdlog::warn("TenantProfileHandler: unknown action {}", static_cast<uint8_t>(req.action));
        res.success = false;
        break;
    }
  } catch (const std::exception &e) {
    spdlog::warn("TenantProfileHandler failed: {}", e.what());
    res.success = false;
  }

  rpc::SendFinalResponse(res, request_version, res_builder);
}

}  // namespace

void Register(replication::RoleReplicaData const &data, system::ReplicaHandlerAccessToState &system_state_access,
              dbms::DbmsHandler &dbms_handler) {
  // NOTE: Register even without license as the user could add a license at run-time
  data.server->rpc_server_.Register<storage::replication::CreateDatabaseRpc>(
      [&data, system_state_access, &dbms_handler](
          std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          auto *req_reader,
          auto *res_builder) mutable {
        CreateDatabaseHandler(system_state_access, data.uuid_, dbms_handler, request_version, req_reader, res_builder);
      });
  data.server->rpc_server_.Register<storage::replication::DropDatabaseRpc>(
      [&data, system_state_access, &dbms_handler](
          std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          auto *req_reader,
          auto *res_builder) mutable {
        DropDatabaseHandler(system_state_access, data.uuid_, dbms_handler, request_version, req_reader, res_builder);
      });
  data.server->rpc_server_.Register<storage::replication::RenameDatabaseRpc>(
      [&data, system_state_access, &dbms_handler](
          std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          auto *req_reader,
          auto *res_builder) mutable {
        RenameDatabaseHandler(system_state_access, data.uuid_, dbms_handler, request_version, req_reader, res_builder);
      });
  data.server->rpc_server_.Register<storage::replication::SuspendDatabaseRpc>(
      [&data, system_state_access, &dbms_handler](
          std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          auto *req_reader,
          auto *res_builder) mutable {
        SuspendDatabaseHandler(system_state_access, data.uuid_, dbms_handler, request_version, req_reader, res_builder);
      });
  data.server->rpc_server_.Register<storage::replication::ResumeDatabaseRpc>(
      [&data, system_state_access, &dbms_handler](
          std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          auto *req_reader,
          auto *res_builder) mutable {
        ResumeDatabaseHandler(system_state_access, data.uuid_, dbms_handler, request_version, req_reader, res_builder);
      });
  data.server->rpc_server_.Register<storage::replication::TenantProfileRpc>(
      [&data, system_state_access, &dbms_handler](
          std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          auto *req_reader,
          auto *res_builder) mutable {
        TenantProfileHandler(system_state_access, data.uuid_, dbms_handler, request_version, req_reader, res_builder);
      });
}
#endif
}  // namespace memgraph::dbms
