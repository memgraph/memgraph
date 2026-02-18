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

#include "rpc/messages.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/replication/recovery.hpp"
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
enum class WriteFilesError : uint8_t { WRITE_FILE_ERROR };

inline auto WriteFilesErrorMsg(WriteFilesError err) -> std::string {
  switch (err) {
    case WriteFilesError::WRITE_FILE_ERROR: {
      return "Failed to load files";
    }
    default:
      std::unreachable();
  }
}

using TransferDurabilityFilesError = std::variant<rpc::RpcError, WriteFilesError>;

inline auto TransferDurabilityFilesErrorMsg(TransferDurabilityFilesError const &global_err) -> std::string {
  return std::visit(utils::Overloaded{[](rpc::RpcError err) { return rpc::GetRpcErrorMsg(err); },
                                      [](WriteFilesError err) { return WriteFilesErrorMsg(err); }},
                    global_err);
}

template <rpc::IsRpc Rpc, typename R, typename... Args>
std::expected<typename Rpc::Response, TransferDurabilityFilesError> TransferDurabilityFiles(
    const R &files, rpc::Client &client, std::filesystem::path const &root_data_dir,
    replication_coordination_glue::ReplicationMode const mode, Args &&...args) {
  utils::MetricsTimer const timer{RpcInfo<Rpc>::timerLabel};

  // if ASYNC mode, we shouldn't block on transferring durability files because there could be a commit task which
  // holds rpc stream and which needs to be executed
  auto maybe_stream_result = (mode == replication_coordination_glue::ReplicationMode::ASYNC)
                                 ? client.TryStream<Rpc>(kRecoveryRpcTimeout, std::forward<Args>(args)...)
                                 : client.Stream<Rpc>(std::forward<Args>(args)...);

  // If dealing with ASYNC replica and couldn't obtain the lock
  if (!maybe_stream_result) {
    return std::unexpected{rpc::RpcError::FAILED_TO_GET_RPC_STREAM};
  }

  slk::Builder *builder = maybe_stream_result->GetBuilder();

  builder->FlushSegment(/*final_segment*/ false, /*force_flush*/ true);

  // If writing files failed, fail the task by returning empty optional
  if (replication::Encoder encoder(builder); !WriteFiles(files, root_data_dir, encoder)) {
    return std::unexpected{WriteFilesError::WRITE_FILE_ERROR};
  }

  return maybe_stream_result->SendAndWaitProgress().transform_error(
      [](rpc::RpcError e) -> TransferDurabilityFilesError { return e; });
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
