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
std::optional<typename T::Response> TransferDurabilityFiles(const R &files, rpc::Client &client, Args &&...args) {
  utils::MetricsTimer const timer{RpcInfo<T>::timerLabel};
  auto stream = client.Stream<T>(std::forward<Args>(args)...);
  if (replication::Encoder encoder(stream.GetBuilder()); !WriteFiles(files, encoder)) {
    return std::nullopt;
  }
  return stream.SendAndWaitProgress();
}

auto GetRecoverySteps(uint64_t replica_commit, utils::FileRetainer::FileLocker *file_locker,
                      const InMemoryStorage *main_storage) -> std::optional<std::vector<RecoveryStep>>;

}  // namespace memgraph::storage
