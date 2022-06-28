// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v2/replication/replication_persistance_helper.hpp"
#include "utils/logging.hpp"

#include <gtest/gtest.h>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>

static int number_of_tests_failed = 0;

class ReplicationPersistanceHelperTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}

  memgraph::storage::replication::ReplicaStatus CreateReplicaStatus(
      std::string name, std::string ip_address, uint16_t port,
      memgraph::storage::replication::ReplicationMode sync_mode, std::optional<double> timeout,
      std::chrono::seconds replica_check_frequency,
      std::optional<memgraph::storage::replication::ReplicationClientConfig::SSL> ssl) const {
    return memgraph::storage::replication::ReplicaStatus{.name = name,
                                                         .ip_address = ip_address,
                                                         .port = port,
                                                         .sync_mode = sync_mode,
                                                         .timeout = timeout,
                                                         .replica_check_frequency = replica_check_frequency,
                                                         .ssl = ssl};
  }

  bool CheckEquality(memgraph::storage::replication::ReplicaStatus &status,
                     memgraph::storage::replication::ReplicaStatus &other_status) {
    if (!CheckEqualityImpl(status, other_status)) {
      ++number_of_tests_failed;
      ExportFailingTest(status);
      return false;
    }

    return true;
  }

  std::vector<std::string> names_ = {"name_01", "name_02"};
  std::vector<std::string> ip_addresses_ = {"192.0.2.146", "168.212.226.204"};
  std::vector<uint16_t> ports_ = {0, 1615, 5001};
  std::vector<memgraph::storage::replication::ReplicationMode> sync_modes_ = {
      memgraph::storage::replication::ReplicationMode::SYNC, memgraph::storage::replication::ReplicationMode::ASYNC};
  std::vector<std::optional<double>> timeouts_ = {std::nullopt, 0, 125, 999.9};
  std::vector<std::chrono::seconds> replica_check_frequencies_ = {std::chrono::seconds(0), std::chrono::seconds(99)};
  std::vector<std::optional<memgraph::storage::replication::ReplicationClientConfig::SSL>> ssls_ = {
      std::nullopt,
      memgraph::storage::replication::ReplicationClientConfig::SSL{
          .key_file = "MFsxCzAJBgNVBAYTAkZSMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJ",
          .cert_file = "20z0qSHpa3YNW6qSp"},
      memgraph::storage::replication::ReplicationClientConfig::SSL{
          .key_file = "+1ZyR4tXgi4+5MHGzhYCIVvHo4hKqYm+J+o5mwQInf1qoAHuO7CLD3WNa1sKcVUV",
          .cert_file = "ZkChSRyc/Whvurx6o85D6qpzywo8xwNaLZHxTQPgcIA5su9ZIytv9LH2E+lSwwID"}};

 private:
  bool CheckEqualityImpl(memgraph::storage::replication::ReplicaStatus &status,
                         memgraph::storage::replication::ReplicaStatus &other_status) {
    if (status.name != other_status.name) {
      return false;
    }
    if (status.ip_address != other_status.ip_address) {
      return false;
    }
    if (status.port != other_status.port) {
      return false;
    }
    if (status.sync_mode != other_status.sync_mode) {
      return false;
    }
    if (status.timeout.has_value() != other_status.timeout.has_value()) {
      return false;
    }
    if (status.timeout.has_value() && (*status.timeout != *other_status.timeout)) {
      return false;
    }
    if (status.replica_check_frequency != other_status.replica_check_frequency) {
      return false;
    }
    if (status.ssl.has_value() != other_status.ssl.has_value()) {
      return false;
    }
    if (status.ssl.has_value()) {
      if (status.ssl->key_file != other_status.ssl->key_file) {
        return false;
      }
      if (status.ssl->cert_file != other_status.ssl->cert_file) {
        return false;
      }
    }

    return true;
  }

  //! Method to automatically generate failing UT.
  void ExportFailingTest(memgraph::storage::replication::ReplicaStatus &status) const {
    std::ofstream myfile;
    myfile.open(fmt::format("replication_persistance_helper_ut_failing_{}.cpp", number_of_tests_failed));
    auto name = status.name;
    auto ip_address = status.ip_address;
    auto port = status.port;
    auto sync_mode = status.sync_mode == memgraph::storage::replication::ReplicationMode::SYNC ? "SYNC" : "ASYNC";
    auto timeout = status.timeout.has_value() ? std::to_string(*status.timeout) : "std::nullopt";
    auto replica_check_frequency = status.replica_check_frequency.count();
    auto ssl = status.ssl.has_value() ? fmt::format(
                                            "memgraph::storage::replication::ReplicationClientConfig::SSL{{.key_file = "
                                            "\"{}\", .cert_file = \"{}\"}}",
                                            status.ssl->key_file, status.ssl->cert_file)
                                      : "std::nullopt";
    auto test = fmt::format(
        "TEST_F(ReplicationPersistanceHelperTest, Test_{}) {{\n"
        "  auto replicas_status = CreateReplicaStatus(\n"
        "      \"{}\", \"{}\", {}, memgraph::storage::replication::ReplicationMode::{}, {}, std::chrono::seconds({}),\n"
        "      {});\n"
        "\n"
        "  auto json_status = memgraph::storage::replication::replica_status_to_json(\n"
        "      memgraph::storage::replication::ReplicaStatus(replicas_status));\n"
        "  auto replicas_status_converted = "
        "memgraph::storage::replication::json_to_replica_status(std::move(json_status));\n"
        "\n"
        "  ASSERT_TRUE(CheckEquality(replicas_status, *replicas_status_converted));\n"
        "}}",
        number_of_tests_failed, name, ip_address, port, sync_mode, timeout, replica_check_frequency, ssl);
    myfile << test;
    myfile.close();
  }

  static_assert(sizeof(memgraph::storage::replication::ReplicaStatus) == 168,
                "Most likely you modified ReplicaStatus without updating the tests. Please modify CheckEqualityImpl, "
                "ExportFailingTest and extend the unit tests with relevant tests.");
};

TEST_F(ReplicationPersistanceHelperTest, BasicTestAllAttributesInitialized) {
  auto replicas_status = CreateReplicaStatus(
      "name", "ip_address", 0, memgraph::storage::replication::ReplicationMode::SYNC, 1.0, std::chrono::seconds(1),
      memgraph::storage::replication::ReplicationClientConfig::SSL{.key_file = "key_file", .cert_file = "cert_file"});

  auto json_status = memgraph::storage::replication::replica_status_to_json(
      memgraph::storage::replication::ReplicaStatus(replicas_status));
  auto replicas_status_converted = memgraph::storage::replication::json_to_replica_status(std::move(json_status));

  ASSERT_TRUE(CheckEquality(replicas_status, *replicas_status_converted));
}

TEST_F(ReplicationPersistanceHelperTest, BasicTestOnlyMandatoryAttributesInitialized) {
  auto replicas_status =
      CreateReplicaStatus("name", "ip_address", 0, memgraph::storage::replication::ReplicationMode::SYNC, std::nullopt,
                          std::chrono::seconds(1), std::nullopt);

  auto json_status = memgraph::storage::replication::replica_status_to_json(
      memgraph::storage::replication::ReplicaStatus(replicas_status));
  auto replicas_status_converted = memgraph::storage::replication::json_to_replica_status(std::move(json_status));

  ASSERT_TRUE(CheckEquality(replicas_status, *replicas_status_converted));
}

TEST_F(ReplicationPersistanceHelperTest, RandomizedTests) {
  auto all_tests_succeeded = true;

  for (auto &name : names_) {
    for (auto &ip_address : ip_addresses_) {
      for (auto port : ports_) {
        for (auto sync_mode : sync_modes_) {
          for (auto timeout : timeouts_) {
            for (auto replica_check_frequency : replica_check_frequencies_) {
              for (auto &ssl : ssls_) {
                auto replicas_status =
                    CreateReplicaStatus(name, ip_address, port, sync_mode, timeout, replica_check_frequency, ssl);

                auto json_status = memgraph::storage::replication::replica_status_to_json(
                    memgraph::storage::replication::ReplicaStatus(replicas_status));
                auto replicas_status_converted =
                    memgraph::storage::replication::json_to_replica_status(std::move(json_status));

                all_tests_succeeded = CheckEquality(replicas_status, *replicas_status_converted) && all_tests_succeeded;
              }
            }
          }
        }
      }
    }
  }

  ASSERT_TRUE(all_tests_succeeded);
}
