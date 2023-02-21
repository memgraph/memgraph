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

// This binary is meant to easily compare the performance of:
//  - Memgraph v2
//  - Memgraph v3
//  - Memgraph v3 with MultiFrame
// This binary measures three things which provides a high level and easily understandable metric about the performance
// difference between the different versions:
//  1. Read time: how much time does it take to read the files:
//  2. Init time: how much time does it take to run the init queries, including the index creation. For details please
//     check RunV2.
//  3. Benchmark time: how much time does it take to run the benchmark queries.
// To quickly compare performance of the different versions just change the query or queries in the benchmark queries
// file you can see the different by running this executable. This way we don't have keep multiple binaries of Memgraph
// v2 and Memgraph v3 with/without MultiFrame, start Memgraph and connect to it with mgconsole and other hassles. As
// everything is run in this binary, it makes easier to generate perf reports/flamegraphs from the query execution of
// different Memgraph versions compared to using the full blown version of Memgraph.
//
// A few important notes:
//  - All the input files are mandated to have an empty line at the end of the file as the reading logic expect that.
//  - tests/mgbench/dataset_creator_unwind.py is recommended to generate the dataset because it generates queries with
//    UNWIND that makes the import faster in Memgraph v3, thus we can compare the performance on non trivial datasets
//    also. To make it possible to use the generated dataset, you have to move the generated index queries into a
//    separate file that can be supplied as index queries file for this binary when using Memgraph v2. The reason for
//    this is Memgraph v3 cannot handle indices yet, thus it crashes.
//  - Check the command line flags and their description defined in this file.
//  - Also check out the --default-multi-frame-size command line flag if you want to play with that.
//  - The log level is manually set to warning in the main function to avoid the overwhelming log messages from Memgraph
//    v3. Apart from ease of use, the huge amount of looging can degrade the actual performance.
//
// Example usage with Memgraph v2:
// ./query_performance
//   --index-queries-file indices.cypher
//   --init-queries-file dataset.cypher
//   --benchmark-queries-files expand.cypher,match.cypyher
//   --use-v3=false
//
// Example usage with Memgraph v3 without MultiFrame:
// ./query_performance
//   --split-file split_file
//   --init-queries-file dataset.cypher
//   --benchmark-queries-files expand.cypher,match.cypyher
//   --use-v3=true
//   --use-multi-frame=false
//
// Example usage with Memgraph v3 with MultiFrame:
// ./query_performance
//   --split-file split_file
//   --init-queries-file dataset.cypher
//   --benchmark-queries-files expand.cypher,match.cypyher
//   --use-v3=true
//   --use-multi-frame=true
//
// The examples are using only the necessary flags, however specifying all of them is not a problem, so if you specify
// --index-queries-file for Memgraph v3, then it will be safely ignored just as --split-file for Memgraph v2.
//
// To generate flamegraph you can use the following command:
// flamegraph --cmd "record -F 997 --call-graph fp -g" --root -o flamegraph.svg -- ./query_performance <flags>
// Using the default option (dwarf) for --call-graph when calling perf might result in too long runtine of flamegraph
// because of address resolution. See https://github.com/flamegraph-rs/flamegraph/issues/74.

#include <chrono>
#include <filesystem>
#include <fstream>
#include <istream>
#include <thread>

#include <gflags/gflags.h>
#include <spdlog/cfg/env.h>
#include <spdlog/spdlog.h>
#include <json/json.hpp>

// v3 includes
#include "io/address.hpp"
#include "io/local_transport/local_system.hpp"
#include "io/message_histogram_collector.hpp"
#include "machine_manager/machine_manager.hpp"
#include "query/discard_value_stream.hpp"
#include "query/v2/discard_value_stream.hpp"
#include "query/v2/interpreter.hpp"
#include "query/v2/request_router.hpp"

// v2 includes
#include "query/interpreter.hpp"
#include "storage/v2/storage.hpp"

// common includes
#include "utils/string.hpp"

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(index_queries_file, "",
              "Path to the file which contains the queries to create indices. Used only for v2. Must contain an empty "
              "line at the end of the file after the queries.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(split_file, "",
              "Path to the split file which contains the predefined labels, properties, edge types and shard-ranges. "
              "Used only for v3. Must contain an empty line at the end of the file.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(init_queries_file, "",
              "Path to the file that is used to insert the initial dataset, one query per line. Must contain an empty "
              "line at the end of the file after the queries.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(benchmark_queries_files, "",
              "Comma separated paths to the files that contain the queries that we want to compare, one query per "
              "line. Must contain an empty line at the end of each file after the queries.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(use_v3, true, "If set to true, then Memgraph v3 will be used, otherwise Memgraph v2 will be used.");

DEFINE_string(export_json_results, "", "If not empty, then the results will be exported as a json file.");

DEFINE_string(data_directory, "mg_data", "Path to directory to use as storage directory for Memgraph v2.");

namespace memgraph::tests::manual {

template <typename TInterpreterContext>
struct DependantTypes {};

template <>
struct DependantTypes<query::InterpreterContext> {
  using Interpreter = query::Interpreter;
  using DiscardValueResultStream = query::DiscardValueResultStream;
};

template <>
struct DependantTypes<query::v2::InterpreterContext> {
  using Interpreter = query::v2::Interpreter;
  using DiscardValueResultStream = query::v2::DiscardValueResultStream;
};

template <typename TRep, typename TPeriod>
void PutResult(nlohmann::json &json, const std::string_view name, std::chrono::duration<TRep, TPeriod> duration) {
  json[name] = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
}

template <typename TInterpreterContext>
using Interpreter = typename DependantTypes<TInterpreterContext>::Interpreter;

template <typename TInterpreterContext>
using DiscardValueResultStream = typename DependantTypes<TInterpreterContext>::DiscardValueResultStream;

template <typename TInterpreterContext>
void RunQueries(TInterpreterContext &interpreter_context, const std::vector<std::string> &queries) {
  Interpreter<TInterpreterContext> interpreter{&interpreter_context};
  DiscardValueResultStream<TInterpreterContext> stream;

  for (const auto &query : queries) {
    auto result = interpreter.Prepare(query, {}, nullptr);
    interpreter.Pull(&stream, std::nullopt, result.qid);
  }
}

template <typename TInterpreterContext>
void RunInitQueries(TInterpreterContext &interpreter_context, const std::vector<std::string> &init_queries) {
  RunQueries(interpreter_context, init_queries);
}

template <typename TInterpreterContext>
void RunBenchmarkQueries(TInterpreterContext &interpreter_context, const std::vector<std::string> &benchmark_queries) {
  RunQueries(interpreter_context, benchmark_queries);
}

std::vector<std::string> ReadQueries(const std::string &file_name) {
  std::vector<std::string> queries{};
  std::string buffer;

  std::ifstream file{file_name, std::ios::in};
  MG_ASSERT(file.good(), "Cannot open queries file to read: {}", file_name);
  while (file.good()) {
    std::getline(file, buffer);
    if (buffer.empty()) {
      continue;
    }
    // Trim the trailing `;`
    queries.push_back(buffer.substr(0, buffer.size() - 1));
  }
  return queries;
}

std::map<std::string, std::vector<std::string>> ReadBenchmarkQueries(const std::string benchmark_queries_files) {
  auto benchmark_files = utils::Split(benchmark_queries_files, ",");
  std::map<std::string, std::vector<std::string>> result;
  for (const auto &benchmark_file : benchmark_files) {
    const auto path = std::filesystem::path(benchmark_file);
    result.emplace(path.stem().string(), ReadQueries(benchmark_file));
  }
  return result;
}

void RunV2() {
  spdlog::critical("Running V2");
  const auto run_start = std::chrono::high_resolution_clock::now();

  const auto index_queries = ReadQueries(FLAGS_index_queries_file);
  const auto init_queries = ReadQueries(FLAGS_init_queries_file);
  const auto benchmarks = ReadBenchmarkQueries(FLAGS_benchmark_queries_files);

  storage::Storage storage{
      storage::Config{.durability{.storage_directory = FLAGS_data_directory,
                                  .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::DISABLED}}};

  memgraph::query::InterpreterContext interpreter_context{
      &storage,
      {.query = {.allow_load_csv = false},
       .execution_timeout_sec = 0,
       .replication_replica_check_frequency = std::chrono::seconds(0),
       .default_kafka_bootstrap_servers = "",
       .default_pulsar_service_url = "",
       .stream_transaction_conflict_retries = 0,
       .stream_transaction_retry_interval = std::chrono::milliseconds(0)},
      FLAGS_data_directory};

  const auto init_start = std::chrono::high_resolution_clock::now();
  RunInitQueries(interpreter_context, index_queries);
  RunInitQueries(interpreter_context, init_queries);
  const auto benchmark_start = std::chrono::high_resolution_clock::now();

  spdlog::critical("Read: {}ms", std::chrono::duration_cast<std::chrono::milliseconds>(init_start - run_start).count());
  spdlog::critical("Init: {}ms",
                   std::chrono::duration_cast<std::chrono::milliseconds>(benchmark_start - init_start).count());

  std::map<std::string, std::chrono::nanoseconds> benchmark_results;
  for (const auto &[name, queries] : benchmarks) {
    const auto current_start = std::chrono::high_resolution_clock::now();
    RunBenchmarkQueries(interpreter_context, queries);
    const auto current_stop = std::chrono::high_resolution_clock::now();
    const auto elapsed = current_stop - current_start;
    spdlog::critical("Benchmark {}: {}ms", name,
                     std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count());
    benchmark_results.emplace(name, elapsed);
  }

  const auto benchmark_end = std::chrono::high_resolution_clock::now();
  spdlog::critical("Benchmark: {}ms",
                   std::chrono::duration_cast<std::chrono::milliseconds>(benchmark_end - benchmark_start).count());

  if (!FLAGS_export_json_results.empty()) {
    nlohmann::json results;
    PutResult(results, "init", benchmark_start - init_start);
    nlohmann::json benchmark_results_json;
    for (const auto &[name, duration] : benchmark_results) {
      PutResult(benchmark_results_json, name, duration);
    }
    results["benchmarks"] = std::move(benchmark_results_json);
    std::ofstream results_file{FLAGS_export_json_results};
    results_file << results.dump();
  }
}

void RunV3() {
  spdlog::critical("Running V3");
  const auto run_start = std::chrono::high_resolution_clock::now();
  std::ifstream sm_file{FLAGS_split_file, std::ios::in};
  MG_ASSERT(sm_file.good(), "Cannot open split file to read: {}", FLAGS_split_file);
  auto sm = memgraph::coordinator::ShardMap::Parse(sm_file);

  const auto init_queries = ReadQueries(FLAGS_init_queries_file);
  const auto benchmarks = ReadBenchmarkQueries(FLAGS_benchmark_queries_files);

  io::local_transport::LocalSystem ls;

  auto unique_local_addr_query = io::Address::UniqueLocalAddress();
  auto io = ls.Register(unique_local_addr_query);

  memgraph::machine_manager::MachineConfig config{
      .coordinator_addresses = std::vector<memgraph::io::Address>{unique_local_addr_query},
      .is_storage = true,
      .is_coordinator = true,
      .listen_ip = unique_local_addr_query.last_known_ip,
      .listen_port = unique_local_addr_query.last_known_port,
      .shard_worker_threads = 2,
  };

  memgraph::coordinator::Coordinator coordinator{sm};

  memgraph::machine_manager::MachineManager<memgraph::io::local_transport::LocalTransport> mm{io, config, coordinator};
  std::jthread mm_thread([&mm] { mm.Run(); });

  auto rr_factory = std::make_unique<memgraph::query::v2::LocalRequestRouterFactory>(io);

  query::v2::InterpreterContext interpreter_context{(memgraph::storage::v3::Shard *)(nullptr),
                                                    {.execution_timeout_sec = 0},
                                                    "data",
                                                    std::move(rr_factory),
                                                    mm.CoordinatorAddress()};

  // without this it fails sometimes because the CreateVertices request might reach the shard worker faster than the
  // ShardToInitialize
  std::this_thread::sleep_for(std::chrono::milliseconds(150));

  const auto init_start = std::chrono::high_resolution_clock::now();
  RunInitQueries(interpreter_context, init_queries);
  const auto benchmark_start = std::chrono::high_resolution_clock::now();

  spdlog::critical("Read: {}ms", std::chrono::duration_cast<std::chrono::milliseconds>(init_start - run_start).count());
  spdlog::critical("Init: {}ms",
                   std::chrono::duration_cast<std::chrono::milliseconds>(benchmark_start - init_start).count());

  std::map<std::string, std::chrono::nanoseconds> benchmark_results;
  for (const auto &[name, queries] : benchmarks) {
    const auto current_start = std::chrono::high_resolution_clock::now();
    RunBenchmarkQueries(interpreter_context, queries);
    const auto current_stop = std::chrono::high_resolution_clock::now();
    const auto elapsed = current_stop - current_start;
    spdlog::critical("Benchmark {}: {}ms", name,
                     std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count());
    benchmark_results.emplace(name, elapsed);
  }

  const auto benchmark_end = std::chrono::high_resolution_clock::now();
  spdlog::critical("Benchmark: {}ms",
                   std::chrono::duration_cast<std::chrono::milliseconds>(benchmark_end - benchmark_start).count());

  ls.ShutDown();

  if (!FLAGS_export_json_results.empty()) {
    nlohmann::json results;
    PutResult(results, "init", benchmark_start - init_start);
    nlohmann::json benchmark_results_json;
    for (const auto &[name, duration] : benchmark_results) {
      PutResult(benchmark_results_json, name, duration);
    }
    results["benchmarks"] = std::move(benchmark_results_json);
    std::ofstream results_file{FLAGS_export_json_results};
    results_file << results.dump();
  }
}
}  // namespace memgraph::tests::manual

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::warn);
  spdlog::cfg::load_env_levels();
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_use_v3) {
    memgraph::tests::manual::RunV3();
  } else {
    memgraph::tests::manual::RunV2();
  }
  return 0;
}
