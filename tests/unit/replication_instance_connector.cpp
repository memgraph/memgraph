// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "coordination/replication_instance_connector.hpp"
#include "coordination/coordinator_instance.hpp"

#include "auth/auth.hpp"
#include "flags/run_time_configurable.hpp"
#include "interpreter_faker.hpp"
#include "io/network/endpoint.hpp"
#include "license/license.hpp"
#include "replication_handler/replication_handler.hpp"
#include "storage/v2/config.hpp"

#include "utils/file.hpp"

#include <gflags/gflags.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "json/json.hpp"

using memgraph::coordination::CoordinatorInstance;
using memgraph::coordination::CoordinatorToReplicaConfig;
using memgraph::coordination::HealthCheckClientCallback;
using memgraph::coordination::HealthCheckInstanceCallback;
using memgraph::coordination::ReplicationClientInfo;
using memgraph::coordination::ReplicationInstanceClient;
using memgraph::coordination::ReplicationInstanceConnector;
using memgraph::io::network::Endpoint;
using memgraph::replication::ReplicationHandler;
using memgraph::replication_coordination_glue::ReplicationMode;
using memgraph::storage::Config;

using testing::_;

class ReplicationInstanceClientMock {};

class ReplicationInstanceConnectorTest : public ::testing::Test {
 public:
  void SetUp() override {}

  void TearDown() override {}
};
