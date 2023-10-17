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
#include <chrono>
#include <string>

namespace memgraph::query {
struct InterpreterConfig {
  struct Query {
    bool allow_load_csv{true};
  } query;

  // The same as \ref memgraph::replication::ReplicationClientConfig
  std::chrono::seconds replication_replica_check_frequency{1};

  std::string default_kafka_bootstrap_servers;
  std::string default_pulsar_service_url;
  uint32_t stream_transaction_conflict_retries;
  std::chrono::milliseconds stream_transaction_retry_interval;
};
}  // namespace memgraph::query
