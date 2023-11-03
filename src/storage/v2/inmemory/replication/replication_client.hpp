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

#include "storage/v2/replication/replication_client.hpp"

namespace memgraph::storage {

class InMemoryStorage;

class InMemoryReplicationClient : public ReplicationClient {
 public:
  InMemoryReplicationClient(const memgraph::replication::ReplicationClientConfig &config);

 protected:
  void RecoverReplica(uint64_t replica_commit, memgraph::storage::Storage *storage) override;

  // TODO: move the GetRecoverySteps stuff below as an internal detail
  using RecoverySnapshot = std::filesystem::path;
  using RecoveryWals = std::vector<std::filesystem::path>;
  struct RecoveryCurrentWal {
    explicit RecoveryCurrentWal(const uint64_t current_wal_seq_num) : current_wal_seq_num(current_wal_seq_num) {}
    uint64_t current_wal_seq_num;
  };
  using RecoveryStep = std::variant<RecoverySnapshot, RecoveryWals, RecoveryCurrentWal>;
  static auto GetRecoverySteps(const uint64_t replica_commit, utils::FileRetainer::FileLocker *file_locker,
                               const InMemoryStorage *storage) -> std::vector<InMemoryReplicationClient::RecoveryStep>;
};

}  // namespace memgraph::storage
