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

// The minimal main/replica instance the replication unit tests run: a DBMS with one database, its replication state
// and a replication handler. Shared between the in-process tests and the process-isolated replica role.

#include <memory>

#include "auth/auth.hpp"
#include "dbms/database.hpp"
#include "dbms/database_protector.hpp"
#include "dbms/dbms_handler.hpp"
#include "parameters/parameters.hpp"
#include "replication/state.hpp"
#include "replication_handler/replication_handler.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "system/system.hpp"
#include "utils/rw_spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::tests {

inline auto MakeCommitArgs(const memgraph::dbms::DatabaseAccess &db_acc) -> memgraph::storage::CommitArgs {
  return memgraph::storage::CommitArgs::make_main(std::make_unique<memgraph::dbms::DatabaseProtector>(db_acc));
}

struct MinMemgraph {
  explicit MinMemgraph(const memgraph::storage::Config &conf)
      : auth{conf.durability.storage_directory / "auth", memgraph::auth::Auth::Config{/* default */}},
        parameters_{conf.durability.storage_directory},
        repl_state{ReplicationStateRootPath(conf)},
        dbms{conf},
        db_acc{dbms.Get()},
        db{*db_acc.get()},
        repl_handler(repl_state, dbms, system_
#ifdef MG_ENTERPRISE
                     ,
                     auth
#endif
                     ,
                     parameters_) {
  }

  auto CreateIndexAccessor() -> std::unique_ptr<memgraph::storage::Storage::Accessor> { return db.ReadOnlyAccess(); }

  auto DropIndexAccessor() -> std::unique_ptr<memgraph::storage::Storage::Accessor> {
    return db.Access(memgraph::storage::StorageAccessType::READ);
  }

  ~MinMemgraph() {
    auto locked_repl_state = repl_state.Lock();
    if (locked_repl_state->IsReplica()) {
      auto &replica_data = std::get<memgraph::replication::RoleReplicaData>(locked_repl_state->ReplicationData());
      replica_data.server.reset();
    } else if (locked_repl_state->IsMain()) {
      auto &main_data = std::get<memgraph::replication::RoleMainData>(locked_repl_state->ReplicationData());
      for (auto &client : main_data.registered_replicas_) {
        client.Shutdown();
      }
      dbms.ForEach([](memgraph::dbms::DatabaseAccess db_acc) {
        auto *storage = db_acc->storage();
        storage->repl_storage_state_.replication_storage_clients_.WithLock([](auto &clients) { clients.clear(); });
      });
    }
  }

  memgraph::auth::SynchedAuth auth;
  memgraph::system::System system_;
  memgraph::parameters::Parameters parameters_;
  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state;
  memgraph::dbms::DbmsHandler dbms;
  memgraph::dbms::DatabaseAccess db_acc;
  memgraph::dbms::Database &db;
  memgraph::replication::ReplicationHandler repl_handler;
};

}  // namespace memgraph::tests
