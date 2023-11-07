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

ReplicationClient::ReplicationClient(const memgraph::replication::ReplicationClientConfig &config, uint64_t id)
    : id_{id},
      name_{config.name},
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

void ReplicationClient::InitializeReplicaState(Storage *storage,
                                               std::atomic<replication::ReplicaState> &replica_state) {
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
    replica_state.store(replication::ReplicaState::UNRESOLVABLE_CONFLICT);
    return;
  }

  current_commit_timestamp = replica.current_commit_timestamp;
  spdlog::trace("Current timestamp on replica {}: {}", name_, current_commit_timestamp);
  spdlog::trace("Current timestamp on main: {}", replStorageState.last_commit_timestamp_.load());
  if (current_commit_timestamp == replStorageState.last_commit_timestamp_.load()) {
    spdlog::debug("Replica '{}' up to date", name_);
    std::unique_lock client_guard{client_lock_};
    replica_state.store(replication::ReplicaState::READY);
  } else {
    spdlog::debug("Replica '{}' is behind", name_);
    {
      std::unique_lock client_guard{client_lock_};
      replica_state.store(replication::ReplicaState::RECOVERY);
    }
    thread_pool_.AddTask([=, this] { this->RecoverReplica(current_commit_timestamp, storage); });
  }
}

TimestampInfo ReplicationClient::GetTimestampInfo(Storage *storage,
                                                  std::atomic<replication::ReplicaState> &replica_state) {
  TimestampInfo info;
  info.current_timestamp_of_replica = 0;
  info.current_number_of_timestamp_behind_master = 0;

  try {
    auto stream{rpc_client_.Stream<replication::TimestampRpc>(storage->id())};
    const auto response = stream.AwaitResponse();
    const auto is_success = response.success;
    if (!is_success) {
      replica_state.store(replication::ReplicaState::RECOVERY_REQUIRED);
      HandleRpcFailure(storage);
    }
    auto main_time_stamp = storage->repl_storage_state_.last_commit_timestamp_.load();
    info.current_timestamp_of_replica = response.current_commit_timestamp;
    info.current_number_of_timestamp_behind_master = response.current_commit_timestamp - main_time_stamp;
  } catch (const rpc::RpcFailedException &) {
    {
      std::unique_lock client_guard(client_lock_);
      replica_state.store(replication::ReplicaState::RECOVERY_REQUIRED);
    }
    HandleRpcFailure(storage);  // mutex already unlocked, if the new enqueued task dispatches immediately it probably
                                // won't block
  }

  return info;
}

void ReplicationClient::HandleRpcFailure(Storage *storage) {
  spdlog::error(utils::MessageWithLink("Couldn't replicate data to {}.", name_, "https://memgr.ph/replication"));
  TryInitializeClientAsync(storage);
}

void ReplicationClient::TryInitializeClientAsync(Storage *storage) {
  thread_pool_.AddTask([=, this] {
    rpc_client_.Abort();
    storage->repl_storage_state_.replication_clients_.WithLock(
        [=, this](ReplicationStorageState::TMP_THING &X) { this->TryInitializeClientSync(storage, X.states_[id_]); });
  });
}

void ReplicationClient::TryInitializeClientSync(Storage *storage,
                                                std::atomic<replication::ReplicaState> &replica_state) {
  try {
    InitializeReplicaState(storage, replica_state);
  } catch (const rpc::VersionMismatchRpcFailedException &) {
    std::unique_lock client_guard{client_lock_};
    replica_state.store(replication::ReplicaState::UNRESOLVABLE_CONFLICT);
    spdlog::error(
        utils::MessageWithLink("Failed to connect to replica {} at the endpoint {}. Because the replica "
                               "deployed is not a compatible version.",
                               name_, rpc_client_.Endpoint(), "https://memgr.ph/replication"));
  } catch (const rpc::RpcFailedException &) {
    std::unique_lock client_guard{client_lock_};
    replica_state.store(replication::ReplicaState::RECOVERY_REQUIRED);
    spdlog::error(utils::MessageWithLink("Failed to connect to replica {} at the endpoint {}.", name_,
                                         rpc_client_.Endpoint(), "https://memgr.ph/replication"));
  }
}

void ReplicationClient::StartTransactionReplication(uint64_t const current_wal_seq_num, Storage *storage,
                                                    std::atomic<replication::ReplicaState> &replica_state) {
  std::unique_lock guard(client_lock_);
  const auto status = replica_state.load();
  switch (status) {
    case replication::ReplicaState::UNRESOLVABLE_CONFLICT:
      spdlog::debug("Replica {} is has data conflicts", name_);
      return;
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
      replica_state.store(replication::ReplicaState::RECOVERY);
      return;
    case replication::ReplicaState::RECOVERY_REQUIRED:
      HandleRpcFailure(storage);
      return;
    case replication::ReplicaState::READY:
      MG_ASSERT(!replica_stream_);
      try {
        replica_stream_.emplace(storage, rpc_client_, current_wal_seq_num);
        replica_state.store(replication::ReplicaState::REPLICATING);
      } catch (const rpc::RpcFailedException &) {
        replica_state.store(replication::ReplicaState::RECOVERY_REQUIRED);
        HandleRpcFailure(storage);
      }
      return;
  }
}

bool ReplicationClient::FinalizeTransactionReplication(Storage *storage,
                                                       std::atomic<replication::ReplicaState> &replica_state) {
  // We can only check the state because it guarantees to be only
  // valid during a single transaction replication (if the assumption
  // that this and other transaction replication functions can only be
  // called from a one thread stands)
  if (replica_state != replication::ReplicaState::REPLICATING) {
    return false;
  }

  if (replica_stream_->IsDefunct()) return false;

  auto task = [storage, this](auto &&setState, auto &&getState) {
    MG_ASSERT(replica_stream_, "Missing stream for transaction deltas");
    try {
      auto response = std::invoke([this]() {
        auto on_exit = utils::OnScopeExit{[this]() { replica_stream_.reset(); }};
        return replica_stream_->Finalize();
      });
      std::unique_lock client_guard(client_lock_);
      if (response.success && getState() != replication::ReplicaState::RECOVERY) {
        setState(replication::ReplicaState::READY);
        return true;
      }
      setState(replication::ReplicaState::RECOVERY);
      thread_pool_.AddTask([=, this] { this->RecoverReplica(response.current_commit_timestamp, storage); });
      return false;
    } catch (const rpc::RpcFailedException &) {
      {
        std::unique_lock client_guard(client_lock_);
        setState(replication::ReplicaState::RECOVERY_REQUIRED);
      }
      HandleRpcFailure(storage);
      return false;
    }
  };

  if (mode_ == memgraph::replication::ReplicationMode::ASYNC) {
    auto asycTask = [storage, task, id = id_] {
      auto setState = [storage, id](replication::ReplicaState newState) {
        storage->repl_storage_state_.replication_clients_.WithLock(
            [id, &newState](ReplicationStorageState::TMP_THING &X) { X.states_[id] = newState; });
      };
      auto getState = [storage, id]() {
        return storage->repl_storage_state_.replication_clients_.WithReadLock(
            [id](ReplicationStorageState::TMP_THING const &X) { return X.states_.at(id).load(); });
      };
      (void)task(setState, getState);
    };
    thread_pool_.AddTask(asycTask);
    return true;
  }

  auto setState = [&](replication::ReplicaState newState) { replica_state = newState; };
  auto getState = [&]() { return replica_state.load(); };
  return task(setState, getState);
}

void ReplicationClient::FrequentCheck(Storage *storage, std::atomic<replication::ReplicaState> &replica_state) {
  const auto network_exists = std::invoke([this]() {
    try {
      auto stream{rpc_client_.Stream<memgraph::replication::FrequentHeartbeatRpc>()};
      const auto response = stream.AwaitResponse();
      return response.success;
    } catch (const rpc::RpcFailedException &) {
      return false;
    }
  });

  if (!network_exists) return;

  // TODO: Subtle race condition, unregister/shutdown will be locking clients
  //       and asking for FrequentCheck to stop

  auto expected = replication::ReplicaState::RECOVERY_REQUIRED;
  bool need_to_recover = replica_state.compare_exchange_strong(expected, replication::ReplicaState::RECOVERY);
  if (!need_to_recover) return;

  TryInitializeClientAsync(storage);
}

replication::ReplicaState ReplicationClient::Start(Storage *storage) {
  auto const &endpoint = rpc_client_.Endpoint();
  spdlog::trace("Replication client started at: {}:{}", endpoint.address, endpoint.port);

  std::atomic<replication::ReplicaState> replica_state = storage::replication::ReplicaState::RECOVERY_REQUIRED;
  TryInitializeClientSync(storage, replica_state);

  return replica_state.load();
}

void ReplicationClient::IfStreamingTransaction(const std::function<void(ReplicaStream &)> &callback, Storage *storage,
                                               std::atomic<replication::ReplicaState> &replica_state) {
  // We can only check the state because it guarantees to be only
  // valid during a single transaction replication (if the assumption
  // that this and other transaction replication functions can only be
  // called from a one thread stands)
  if (replica_state != replication::ReplicaState::REPLICATING) {
    return;
  }

  if (replica_stream_->IsDefunct()) return;

  try {
    callback(*replica_stream_);  // failure state what if not streaming (std::nullopt)
  } catch (const rpc::RpcFailedException &) {
    {
      std::unique_lock client_guard{client_lock_};
      replica_state.store(replication::ReplicaState::RECOVERY_REQUIRED);
    }
    HandleRpcFailure(storage);
  }
}
void ReplicationClient::StartFrequentCheck(Storage *storage, std::atomic<replication::ReplicaState> &replica_state) {
  if (replica_check_frequency_ > std::chrono::seconds(0)) {
    replica_checker_.Run("Replica Checker", replica_check_frequency_,
                         [=, this, replica_state = &replica_state] { this->FrequentCheck(storage, *replica_state); });
  }
}
void ReplicationClient::StopFrequentCheck() { replica_checker_.Stop(); }

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
