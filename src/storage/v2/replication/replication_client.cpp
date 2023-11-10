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
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"

namespace memgraph::storage {

static auto CreateClientContext(const memgraph::replication::ReplicationClientConfig &config)
    -> communication::ClientContext {
  return (config.ssl) ? communication::ClientContext{config.ssl->key_file, config.ssl->cert_file}
                      : communication::ClientContext{};
}

ReplicationClient::ReplicationClient(const memgraph::replication::ReplicationClientConfig &config)
    : name_{config.name},
      rpc_context_{CreateClientContext(config)},
      rpc_client_{io::network::Endpoint(io::network::Endpoint::needs_resolving, config.ip_address, config.port),
                  &rpc_context_},
      replica_check_frequency_{config.replica_check_frequency},
      mode_{config.mode} {}

ReplicationClient::~ReplicationClient() {
  auto endpoint = rpc_client_.Endpoint();
  spdlog::trace("Closing replication client on {}:{}", endpoint.address, endpoint.port);
  thread_pool_.Shutdown();
}

void ReplicationClient::CheckReplicaState(Storage *storage) {
  uint64_t current_commit_timestamp{kTimestampInitialId};

  auto &replStorageState = storage->repl_storage_state_;
  auto stream{rpc_client_.Stream<replication::HeartbeatRpc>(storage->id(), replStorageState.last_commit_timestamp_,
                                                            std::string{replStorageState.epoch_.id()})};

  const auto replica = stream.AwaitResponse();
  std::optional<uint64_t> branching_point;
  if (replica.epoch_id != replStorageState.epoch_.id() && replica.current_commit_timestamp != kTimestampInitialId) {
    auto const &history = replStorageState.history;
    const auto epoch_info_iter = std::find_if(history.crbegin(), history.crend(), [&](const auto &main_epoch_info) {
      return main_epoch_info.first == replica.epoch_id;
    });
    if (epoch_info_iter == history.crend()) {
      branching_point = 0;
    } else if (epoch_info_iter->second != replica.current_commit_timestamp) {
      branching_point = epoch_info_iter->second;
    }
  }
  if (branching_point) {
    spdlog::error(
        "You cannot register Replica {} to this Main because at one point "
        "Replica {} acted as the Main instance. Both the Main and Replica {} "
        "now hold unique data. Please resolve data conflicts and start the "
        "replication on a clean instance.",
        name_, name_, name_);
    return;
  }

  current_commit_timestamp = replica.current_commit_timestamp;
  spdlog::trace("Current timestamp on replica {}: {}", name_, current_commit_timestamp);
  spdlog::trace("Current timestamp on main: {}", replStorageState.last_commit_timestamp_.load());
  std::unique_lock client_guard{state_lock_};
  if (current_commit_timestamp == replStorageState.last_commit_timestamp_.load()) {
    spdlog::debug("Replica '{}' up to date", name_);
    replica_state_.store(replication::ReplicaState::READY);
  } else {
    spdlog::debug("Replica '{}' is behind", name_);
    replica_state_.store(replication::ReplicaState::RECOVERY);
    thread_pool_.AddTask([=, this] { this->RecoverReplica(current_commit_timestamp, storage); });
  }
}

TimestampInfo ReplicationClient::GetTimestampInfo(Storage *storage) {
  TimestampInfo info;
  info.current_timestamp_of_replica = 0;
  info.current_number_of_timestamp_behind_master = 0;

  try {
    auto stream{rpc_client_.Stream<replication::TimestampRpc>(storage->id())};
    const auto response = stream.AwaitResponse();
    const auto is_success = response.success;
    if (!is_success) {
      std::unique_lock client_guard(state_lock_);
      replica_state_.store(replication::ReplicaState::MAYBE_BEHIND);
      LogRpcFailure();
    }
    auto main_time_stamp = storage->repl_storage_state_.last_commit_timestamp_.load();
    info.current_timestamp_of_replica = response.current_commit_timestamp;
    info.current_number_of_timestamp_behind_master = response.current_commit_timestamp - main_time_stamp;
  } catch (const rpc::RpcFailedException &) {
    {
      std::unique_lock client_guard(state_lock_);
      replica_state_.store(replication::ReplicaState::MAYBE_BEHIND);
    }
    LogRpcFailure();  // mutex already unlocked, if the new enqueued task dispatches immediately it probably
                      // won't block
  }

  return info;
}

void ReplicationClient::LogRpcFailure() {
  spdlog::error(utils::MessageWithLink("Couldn't replicate data to {}.", name_, "https://memgr.ph/replication"));
  // No need to proactively try to recover, this will be done by the next commit or the frequent checker
  // TryInitializeClientAsync(storage);
}

void ReplicationClient::TryCheckReplicaStateAsync(Storage *storage) {
  thread_pool_.AddTask([=, this] {
    // rpc_client_.Abort();
    this->TryCheckReplicaStateSync(storage);
  });
}

void ReplicationClient::TryCheckReplicaStateSync(Storage *storage) {
  try {
    CheckReplicaState(storage);
  } catch (const rpc::VersionMismatchRpcFailedException &) {
    std::unique_lock client_guard{state_lock_};
    replica_state_.store(replication::ReplicaState::MAYBE_BEHIND);
    spdlog::error(
        utils::MessageWithLink("Failed to connect to replica {} at the endpoint {}. Because the replica "
                               "deployed is not a compatible version.",
                               name_, rpc_client_.Endpoint(), "https://memgr.ph/replication"));
  } catch (const rpc::RpcFailedException &) {
    std::unique_lock client_guard{state_lock_};
    replica_state_.store(replication::ReplicaState::MAYBE_BEHIND);
    spdlog::error(utils::MessageWithLink("Failed to connect to replica {} at the endpoint {}.", name_,
                                         rpc_client_.Endpoint(), "https://memgr.ph/replication"));
  }
}

void ReplicationClient::StartTransactionReplication(const uint64_t current_wal_seq_num, Storage *storage) {
  std::unique_lock guard(state_lock_);
  const auto status = replica_state_.load();
  switch (status) {
    case replication::ReplicaState::RECOVERY:
      spdlog::debug("Replica {} is behind MAIN instance", name_);
      return;
    case replication::ReplicaState::REPLICATING:
      spdlog::debug("Replica {} missed a transaction", name_);
      // We missed a transaction because we're still replicating
      // the previous transaction so we need to go to RECOVERY
      // state to catch up with the missing transaction
      // We cannot queue the recovery process here because
      // an error can happen while we're replicating the previous
      // transaction after which the client should go to
      // INVALID state before starting the recovery process
      //
      // This is a signal to any async streams that are still finalizing to start recovery, since this commit will be
      // missed.
      replica_state_.store(replication::ReplicaState::RECOVERY);
      return;
    case replication::ReplicaState::MAYBE_BEHIND:
      spdlog::error(utils::MessageWithLink("Couldn't replicate data to {}.", name_, "https://memgr.ph/replication"));
      TryCheckReplicaStateAsync(storage);
      return;
    case replication::ReplicaState::READY:
      MG_ASSERT(!replica_stream_);
      try {
        replica_stream_.emplace(storage, rpc_client_, current_wal_seq_num);
        replica_state_.store(replication::ReplicaState::REPLICATING);
      } catch (const rpc::RpcFailedException &) {
        replica_state_.store(replication::ReplicaState::MAYBE_BEHIND);
        LogRpcFailure();
      }
      return;
  }
}

bool ReplicationClient::FinalizeTransactionReplication(Storage *storage) {
  // We can only check the state because it guarantees to be only
  // valid during a single transaction replication (if the assumption
  // that this and other transaction replication functions can only be
  // called from a one thread stands)
  if (replica_state_ != replication::ReplicaState::REPLICATING) {
    return false;
  }

  if (replica_stream_->IsDefunct()) return false;

  auto task = [=, this]() {
    MG_ASSERT(replica_stream_, "Missing stream for transaction deltas");
    try {
      auto response = replica_stream_->Finalize();
      std::unique_lock client_guard(state_lock_);
      replica_stream_.reset();
      if (!response.success || replica_state_ == replication::ReplicaState::RECOVERY) {
        replica_state_.store(replication::ReplicaState::RECOVERY);
        thread_pool_.AddTask([=, this] { this->RecoverReplica(response.current_commit_timestamp, storage); });
      } else {
        replica_state_.store(replication::ReplicaState::READY);
        return true;
      }
    } catch (const rpc::RpcFailedException &) {
      std::unique_lock client_guard(state_lock_);
      replica_stream_.reset();
      replica_state_.store(replication::ReplicaState::MAYBE_BEHIND);
      LogRpcFailure();
    }
    return false;
  };

  if (mode_ == memgraph::replication::ReplicationMode::ASYNC) {
    thread_pool_.AddTask([=] { (void)task(); });
    return true;
  }

  return task();
}

void ReplicationClient::FrequentCheck(Storage *storage) {
  const auto is_success = std::invoke([this]() {
    try {
      auto stream{rpc_client_.Stream<memgraph::replication::FrequentHeartbeatRpc>()};
      const auto response = stream.AwaitResponse();
      return response.success;
    } catch (const rpc::RpcFailedException &) {
      return false;
    }
  });
  // States: READY, REPLICATING, RECOVERY, INVALID
  // If success && ready, replicating, recovery -> stay the same because something good is going on.
  // If success && INVALID -> [it's possible that replica came back to life] -> TryInitializeClient.
  // If fail -> [replica is not reachable at all] -> INVALID state.
  // NOTE: TryInitializeClient might return nothing if there is a branching point.
  // NOTE: The early return pattern simplified the code, but the behavior should be as explained.
  if (!is_success) {
    std::unique_lock client_guard{state_lock_};
    replica_state_.store(replication::ReplicaState::MAYBE_BEHIND);
    return;
  }
  if (replica_state_.load() == replication::ReplicaState::MAYBE_BEHIND) {
    TryCheckReplicaStateAsync(storage);
  }
}

void ReplicationClient::Start(Storage *storage) {
  auto const &endpoint = rpc_client_.Endpoint();
  spdlog::trace("Replication client started at: {}:{}", endpoint.address, endpoint.port);

  TryCheckReplicaStateSync(storage);

  // Help the user to get the most accurate replica state possible.
  if (replica_check_frequency_ > std::chrono::seconds(0)) {
    replica_checker_.Run("Replica Checker", replica_check_frequency_, [=, this] { this->FrequentCheck(storage); });
  }
}

void ReplicationClient::IfStreamingTransaction(const std::function<void(ReplicaStream &)> &callback, Storage *storage) {
  // We can only check the state because it guarantees to be only
  // valid during a single transaction replication (if the assumption
  // that this and other transaction replication functions can only be
  // called from a one thread stands)
  if (replica_state_ != replication::ReplicaState::REPLICATING) {
    return;
  }

  if (replica_stream_->IsDefunct()) return;

  try {
    callback(*replica_stream_);  // failure state what if not streaming (std::nullopt)
  } catch (const rpc::RpcFailedException &) {
    std::unique_lock client_guard{state_lock_};
    replica_state_.store(replication::ReplicaState::MAYBE_BEHIND);
    LogRpcFailure();
  }
}

////// ReplicaStream //////
ReplicaStream::ReplicaStream(Storage *storage, rpc::Client &rpc_client, const uint64_t current_seq_num)
    : storage_{storage},
      stream_(rpc_client.Stream<replication::AppendDeltasRpc>(
          storage->id(), storage->repl_storage_state_.last_commit_timestamp_.load(), current_seq_num)) {
  replication::Encoder encoder{stream_.GetBuilder()};
  encoder.WriteString(storage->repl_storage_state_.epoch_.id());
}

void ReplicaStream::AppendDelta(const Delta &delta, const Vertex &vertex, uint64_t final_commit_timestamp) {
  replication::Encoder encoder(stream_.GetBuilder());
  EncodeDelta(&encoder, storage_->name_id_mapper_.get(), storage_->config_.items, delta, vertex,
              final_commit_timestamp);
}

void ReplicaStream::AppendDelta(const Delta &delta, const Edge &edge, uint64_t final_commit_timestamp) {
  replication::Encoder encoder(stream_.GetBuilder());
  EncodeDelta(&encoder, storage_->name_id_mapper_.get(), delta, edge, final_commit_timestamp);
}

void ReplicaStream::AppendTransactionEnd(uint64_t final_commit_timestamp) {
  replication::Encoder encoder(stream_.GetBuilder());
  EncodeTransactionEnd(&encoder, final_commit_timestamp);
}

void ReplicaStream::AppendOperation(durability::StorageMetadataOperation operation, LabelId label,
                                    const std::set<PropertyId> &properties, const LabelIndexStats &stats,
                                    const LabelPropertyIndexStats &property_stats, uint64_t timestamp) {
  replication::Encoder encoder(stream_.GetBuilder());
  EncodeOperation(&encoder, storage_->name_id_mapper_.get(), operation, label, properties, stats, property_stats,
                  timestamp);
}

replication::AppendDeltasRes ReplicaStream::Finalize() { return stream_.AwaitResponse(); }

}  // namespace memgraph::storage
