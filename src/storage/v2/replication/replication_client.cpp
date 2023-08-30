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

#include "storage/v2/replication/replication_client.hpp"

#include <algorithm>
#include <type_traits>

#include "storage/v2/durability/durability.hpp"
#include "storage/v2/replication/config.hpp"
#include "storage/v2/replication/enums.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/transaction.hpp"
#include "utils/file_locker.hpp"
#include "utils/logging.hpp"
#include "utils/message.hpp"

namespace memgraph::storage {

static auto CreateClientContext(const replication::ReplicationClientConfig &config) -> communication::ClientContext {
  return (config.ssl) ? communication::ClientContext{config.ssl->key_file, config.ssl->cert_file}
                      : communication::ClientContext{};
}

ReplicationClient::ReplicationClient(std::string name, memgraph::io::network::Endpoint endpoint,
                                     replication::ReplicationMode mode,
                                     replication::ReplicationClientConfig const &config)
    : name_{std::move(name)},
      rpc_context_{CreateClientContext(config)},
      rpc_client_{std::move(endpoint), &rpc_context_},
      replica_check_frequency_{config.replica_check_frequency},
      mode_{mode} {}

ReplicationClient::~ReplicationClient() {
  auto endpoint = rpc_client_.Endpoint();
  spdlog::trace("Closing replication client on {}:{}", endpoint.address, endpoint.port);
}

////// ReplicaStream //////
ReplicaStream::ReplicaStream(ReplicationClient *self, const uint64_t previous_commit_timestamp,
                             const uint64_t current_seq_num)
    : self_(self),
      stream_(self_->rpc_client_.Stream<replication::AppendDeltasRpc>(previous_commit_timestamp, current_seq_num)) {
  replication::Encoder encoder{stream_.GetBuilder()};
  encoder.WriteString(self_->GetEpochId());
}

void ReplicaStream::AppendDelta(const Delta &delta, const Vertex &vertex, uint64_t final_commit_timestamp) {
  replication::Encoder encoder(stream_.GetBuilder());
  auto *storage = self_->GetStorage();
  EncodeDelta(&encoder, storage->name_id_mapper_.get(), storage->config_.items, delta, vertex, final_commit_timestamp);
}

void ReplicaStream::AppendDelta(const Delta &delta, const Edge &edge, uint64_t final_commit_timestamp) {
  replication::Encoder encoder(stream_.GetBuilder());
  EncodeDelta(&encoder, self_->GetStorage()->name_id_mapper_.get(), delta, edge, final_commit_timestamp);
}

void ReplicaStream::AppendTransactionEnd(uint64_t final_commit_timestamp) {
  replication::Encoder encoder(stream_.GetBuilder());
  EncodeTransactionEnd(&encoder, final_commit_timestamp);
}

void ReplicaStream::AppendOperation(durability::StorageGlobalOperation operation, LabelId label,
                                    const std::set<PropertyId> &properties, uint64_t timestamp) {
  replication::Encoder encoder(stream_.GetBuilder());
  EncodeOperation(&encoder, self_->GetStorage()->name_id_mapper_.get(), operation, label, properties, timestamp);
}

replication::AppendDeltasRes ReplicaStream::Finalize() { return stream_.AwaitResponse(); }

}  // namespace memgraph::storage
