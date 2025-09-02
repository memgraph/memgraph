// Copyright 2025 Memgraph Ltd.
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

#include "rpc/messages.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/replication/recovery.hpp"
#include "storage/v2/replication/replication_client.hpp"
#include "utils/event_histogram.hpp"
#include "utils/metrics_timer.hpp"

namespace memgraph::metrics {
extern const Event SnapshotRpc_us;
extern const Event WalFilesRpc_us;
extern const Event CurrentWalRpc_us;
}  // namespace memgraph::metrics

namespace memgraph::storage {

template <rpc::IsRpc T>
struct RpcInfo {
  static const metrics::Event timerLabel;
};

constexpr auto kRecoveryRpcTimeout = std::chrono::milliseconds(5000);

class InMemoryStorage;

template <typename T>
std::enable_if_t<std::is_same_v<T, std::filesystem::path>, bool> WriteFiles(const T &path,
                                                                            replication::Encoder &encoder) {
  if (!encoder.WriteFile(path)) {
    spdlog::error("File {} couldn't be loaded so it won't be transferred to the replica.", path);
    return false;
  }
  return true;
}

template <typename T>
std::enable_if_t<std::is_same_v<T, std::vector<std::filesystem::path>>, bool> WriteFiles(
    const T &paths, replication::Encoder &encoder) {
  for (const auto &path : paths) {
    if (!encoder.WriteFile(path)) {
      spdlog::error("File {} couldn't be loaded so it won't be transferred to the replica.", path);
      return false;
    }
    spdlog::debug("Loaded file: {}", path);
  }
  return true;
}

template <rpc::IsRpc T, typename R, typename... Args>
std::optional<typename T::Response> TransferDurabilityFiles(const R &files, rpc::Client &client,
                                                            replication_coordination_glue::ReplicationMode const mode,
                                                            Args &&...args) {
  utils::MetricsTimer const timer{RpcInfo<T>::timerLabel};
  std::optional<rpc::Client::StreamHandler<T>> maybe_stream_result;

  // if ASYNC mode, we shouldn't block on transferring durability files because there could be a commit task which holds
  // rpc stream and which needs to be executed
  if (mode == replication_coordination_glue::ReplicationMode::ASYNC) {
    maybe_stream_result = client.TryStream<T>(kRecoveryRpcTimeout, std::forward<Args>(args)...);
  } else {
    // in SYNC and STRICT_SYNC mode, we block until we obtain RPC lock
    maybe_stream_result.emplace(client.Stream<T>(std::forward<Args>(args)...));
  }

  // If dealing with ASYNC replica and couldn't obtain the lock
  if (!maybe_stream_result.has_value()) {
    return std::nullopt;
  }

  slk::Builder *builder = maybe_stream_result->GetBuilder();

  // Flush the segment so the file data could start at the beginning of the next segment
  builder->PrepareForFileSending();

  // If writing files failed, fail the task by returning empty optional
  if (replication::Encoder encoder(builder); !WriteFiles(files, encoder)) {
    return std::nullopt;
  }

  return maybe_stream_result->SendAndWaitProgress();
}

auto GetRecoverySteps(uint64_t replica_commit, utils::FileRetainer::FileLocker *file_locker,
                      const InMemoryStorage *main_storage) -> std::optional<std::vector<RecoveryStep>>;

}  // namespace memgraph::storage
