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

#include <chrono>
#include <string>
#include <system_error>
#include <type_traits>

#include "metrics/scoped_histogram_timer.hpp"
#include "rpc/messages.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/replication/recovery.hpp"

namespace memgraph::storage {
template <rpc::IsRpc T>
struct RpcInfo {
  static prometheus::Histogram *histogram();
  static void ObserveThroughput(std::string const &instance_name, double bytes_per_second);
};

constexpr auto kRecoveryRpcTimeout = std::chrono::milliseconds(5000);

class InMemoryStorage;

struct WalChainInfo {
  bool covered_by_wals;
  uint64_t prev_seq_num;
  uint64_t first_useful_wal;
};

inline auto GetFilePathWithoutDataDir(std::filesystem::path const &orig, std::filesystem::path const &root_data_dir)
    -> std::filesystem::path {
  auto rel = std::filesystem::relative(orig, root_data_dir);
  if (rel.string().starts_with("..")) {
    throw std::invalid_argument("Path not under data directory");
  }
  return rel;
}

template <typename T>
  requires(std::is_same_v<T, std::filesystem::path>)
bool WriteFiles(const T &path, std::filesystem::path const &root_data_dir, replication::Encoder &encoder) {
  if (!encoder.WriteFile(path, GetFilePathWithoutDataDir(path, root_data_dir))) {
    spdlog::error("File {} couldn't be loaded so it won't be transferred to the replica.", path);
    return false;
  }
  return true;
}

template <typename T>
  requires(std::is_same_v<T, std::vector<std::filesystem::path>>)
bool WriteFiles(const T &paths, std::filesystem::path const &root_data_dir, replication::Encoder &encoder) {
  for (const auto &path : paths) {
    // Flush the segment so the file data could start at the beginning of the next segment
    if (!encoder.WriteFile(path, GetFilePathWithoutDataDir(path, root_data_dir))) {
      spdlog::error("File {} couldn't be loaded so it won't be transferred to the replica.", path);
      return false;
    }
    spdlog::debug("Loaded file: {}", path);
  }
  return true;
}

template <typename T>
  requires(std::is_same_v<T, std::filesystem::path>)
auto TotalFileBytes(const T &path) -> std::optional<uint64_t> {
  std::error_code ec;
  auto const size = std::filesystem::file_size(path, ec);
  return ec ? uint64_t{0} : size;
}

template <typename T>
  requires std::is_same_v<T, std::vector<std::filesystem::path>>
auto TotalFileBytes(const T &paths) -> std::optional<uint64_t> {
  uint64_t total = 0;
  for (auto const &path : paths) {
    std::error_code ec;
    auto const size = std::filesystem::file_size(path, ec);
    if (ec) return std::nullopt;  // genuinely stops here
    total += size;
  }
  return total;
}

template <rpc::IsRpc T, typename R, typename... Args>
std::optional<typename T::Response> TransferDurabilityFiles(const R &files, rpc::Client &client,
                                                            std::filesystem::path const &root_data_dir,
                                                            replication_coordination_glue::ReplicationMode const mode,
                                                            std::string const &instance_name, Args &&...args) {
  metrics::ScopedHistogramTimer const timer{RpcInfo<T>::histogram()};
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
  if (!maybe_stream_result) {
    return std::nullopt;
  }

  // intentionally don't take into account waiting for RPC lock
  auto const transfer_start = std::chrono::steady_clock::now();

  slk::Builder *builder = maybe_stream_result->GetBuilder();
  builder->FlushSegment(/*final_segment*/ false, /*force_flush*/ true);

  // If writing files failed, fail the task by returning empty optional
  if (replication::Encoder encoder(builder); !WriteFiles(files, root_data_dir, encoder)) {
    return std::nullopt;
  }

  auto response = maybe_stream_result->SendAndWaitProgress();

  // Record per-instance recovery throughput (bytes/s) so a slow replication link can be identified.
  auto const elapsed_seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - transfer_start).count();
  if (auto const total_bytes = TotalFileBytes(files);
      total_bytes.has_value() && *total_bytes > 0 && elapsed_seconds > 0.0) {
    RpcInfo<T>::ObserveThroughput(instance_name, static_cast<double>(*total_bytes) / elapsed_seconds);
  }

  return response;
}

auto GetRecoverySteps(uint64_t replica_commit, utils::FileRetainer::FileLocker *file_locker,
                      const InMemoryStorage *main_storage) -> std::optional<std::vector<RecoveryStep>>;

auto GetLatestSnapshot(const InMemoryStorage *main_storage) -> std::optional<durability::SnapshotDurabilityInfo>;

// Checks what part of the WAL chain is needed
auto GetWalChainInfo(std::vector<durability::WalDurabilityInfo> const &wal_files, uint64_t replica_commit)
    -> WalChainInfo;

auto FirstWalAfterSnapshot(std::vector<durability::WalDurabilityInfo> const &wal_files, uint64_t snap_durable_ts,
                           uint64_t first_useful_wal) -> uint64_t;

// Copy and lock the chain part we need, from oldest to newest
auto GetRecoveryWalFiles(utils::FileRetainer::FileLockerAccessor *locker_acc,
                         std::vector<durability::WalDurabilityInfo> const &wal_files, uint64_t first_useful_wal)
    -> std::optional<RecoveryWals>;

}  // namespace memgraph::storage
