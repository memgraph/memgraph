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

#include "utils/event_histogram.hpp"

// clang-format off
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define GenerateRpcTimer(RPC) \
  M(RPC##_us, HighAvailability, "Latency of sending and waiting for response from " #RPC " in microseconds", 50, 90, 99)

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define APPLY_FOR_HISTOGRAMS(M)                                                                                   \
  M(QueryExecutionLatency_us, Query, "Query execution latency in microseconds", 50, 90, 99)                       \
  M(SnapshotCreationLatency_us, Snapshot, "Snapshot creation latency in microseconds", 50, 90, 99)                \
  M(SnapshotRecoveryLatency_us, Snapshot, "Snapshot recovery latency in microseconds", 50, 90, 99)                \
  M(InstanceSuccCallback_us, HighAvailability, "Instance success callback in microseconds", 50, 90, 99)           \
  M(InstanceFailCallback_us, HighAvailability, "Instance failure callback in microseconds", 50, 90, 99)           \
  M(ChooseMostUpToDateInstance_us, HighAvailability, "Latency of choosing next main in microseconds", 50, 90, 99) \
  M(SocketConnect_us, General, "Latency of Socket::Connect in microseconds", 50, 90, 99)                          \
  M(ReplicaStream_us, HighAvailability, "Latency of creating replica stream in microseconds", 50, 90, 99)         \
  M(DataFailover_us, HighAvailability, "Latency of the failover procedure in microseconds", 50, 90, 99)           \
  M(StartTxnReplication_us, HighAvailability, "Latency of starting txn replication in us", 50, 90, 99)            \
  M(FinalizeTxnReplication_us, HighAvailability, "Latency of finishing txn replication in us", 50, 90, 99)        \
  GenerateRpcTimer(PromoteToMainRpc)                                                                              \
  GenerateRpcTimer(DemoteMainToReplicaRpc)                                                                        \
  GenerateRpcTimer(RegisterReplicaOnMainRpc)                                                                      \
  GenerateRpcTimer(UnregisterReplicaRpc)                                                                          \
  GenerateRpcTimer(EnableWritingOnMainRpc)                                                                        \
  GenerateRpcTimer(StateCheckRpc)                                                                                 \
  GenerateRpcTimer(GetDatabaseHistoriesRpc)                                                                       \
  GenerateRpcTimer(HeartbeatRpc)                                                                                  \
  GenerateRpcTimer(PrepareCommitRpc)                                                                              \
  GenerateRpcTimer(SnapshotRpc)                                                                                   \
  GenerateRpcTimer(CurrentWalRpc)                                                                                 \
  GenerateRpcTimer(WalFilesRpc)                                                                                   \
  GenerateRpcTimer(FrequentHeartbeatRpc)                                                                          \
  GenerateRpcTimer(SystemRecoveryRpc)                                                                             \
  M(GetHistories_us, HighAvailability, "Latency of retrieving instances' history in microseconds", 50, 90, 99)
// clang-format on

namespace memgraph::metrics {

// define every Event as an index in the array of counters
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define M(NAME, TYPE, DOCUMENTATION, ...) extern const Event NAME = __COUNTER__;
APPLY_FOR_HISTOGRAMS(M)
#undef M

inline constexpr Event END = __COUNTER__;

// Initialize array for the global histogram with all named histograms and their percentiles
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
Histogram global_histograms_array[END]{

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define M(NAME, TYPE, DOCUMENTATION, ...) Histogram({__VA_ARGS__}),
    APPLY_FOR_HISTOGRAMS(M)
#undef M
};

// Initialize global histograms
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
EventHistograms global_histograms(global_histograms_array);

const Event EventHistograms::num_histograms = END;

void Measure(const Event event, Value const value) { global_histograms.Measure(event, value); }

void EventHistograms::Measure(const Event event, Value const value) { histograms_[event].Measure(value); }

const char *GetHistogramName(const Event event) {
  static const char *strings[] = {
  // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define M(NAME, TYPE, DOCUMENTATION, ...) #NAME,
      APPLY_FOR_HISTOGRAMS(M)
#undef M
  };

  return strings[event];
}

const char *GetHistogramDocumentation(const Event event) {
  static const char *strings[] = {
  // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define M(NAME, TYPE, DOCUMENTATION, ...) DOCUMENTATION,
      APPLY_FOR_HISTOGRAMS(M)
#undef M
  };

  return strings[event];
}

const char *GetHistogramType(const Event event) {
  static const char *strings[] = {
  // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define M(NAME, TYPE, DOCUMENTATION, ...) #TYPE,
      APPLY_FOR_HISTOGRAMS(M)
#undef M
  };

  return strings[event];
}

Event HistogramEnd() { return END; }
}  // namespace memgraph::metrics
