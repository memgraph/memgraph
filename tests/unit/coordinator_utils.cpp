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

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "coordination/coordinator_instance.hpp"
#include "utils/functional.hpp"

using memgraph::coordination::CoordinatorInstance;
using memgraph::replication_coordination_glue::InstanceDBInfo;
using memgraph::replication_coordination_glue::InstanceInfo;
using memgraph::utils::UUID;

TEST(CoordinationUtils, FailoverFirstInstanceNewest) {
  // Choose any if everything is same

  auto const default_db_uuid = std::string{UUID()};

  std::map<std::string, InstanceInfo> instances_info{
      {"instance_1",
       InstanceInfo{.dbs_info = std::vector{InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 51}},
                    .last_committed_system_timestamp = 1}},
      {"instance_2",
       InstanceInfo{.dbs_info = std::vector{InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 51}},
                    .last_committed_system_timestamp = 1

       }},
      {"instance_3",
       InstanceInfo{.dbs_info = std::vector{InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 51}},
                    .last_committed_system_timestamp = 1}},

  };

  auto const maybe_instance_name = CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_TRUE(*maybe_instance_name == "instance_1" || *maybe_instance_name == "instance_2" ||
              *maybe_instance_name == "instance_3");
}

TEST(CoordinationUtils, FailoverSecondInstanceNewest) {
  // Instanc2 newest

  auto const default_db_uuid = std::string{UUID()};

  std::map<std::string, InstanceInfo> instances_info{
      {"instance_1",
       InstanceInfo{.dbs_info = std::vector{InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 51}},
                    .last_committed_system_timestamp = 1}},
      {"instance_2",
       InstanceInfo{.dbs_info = std::vector{InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 53}},
                    .last_committed_system_timestamp = 1

       }},
      {"instance_3",
       InstanceInfo{.dbs_info = std::vector{InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 51}},
                    .last_committed_system_timestamp = 1}},

  };

  auto const maybe_instance_name = CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_TRUE(*maybe_instance_name == "instance_2");
}

TEST(CoordinationUtils, FailoverLastInstanceNewest) {
  // Prioritize one with the largest last durable timestamp on

  auto const default_db_uuid = std::string{UUID()};

  std::map<std::string, InstanceInfo> instances_info{
      {"instance_1",
       InstanceInfo{.dbs_info = std::vector{InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 51}},
                    .last_committed_system_timestamp = 1}},
      {"instance_2",
       InstanceInfo{.dbs_info = std::vector{InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 57}},
                    .last_committed_system_timestamp = 1

       }},
      {"instance_3",
       InstanceInfo{.dbs_info = std::vector{InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 59}},
                    .last_committed_system_timestamp = 1}},

  };

  auto const maybe_instance_name = CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_TRUE(*maybe_instance_name == "instance_3");
}

TEST(CoordinationUtils, MTFailover1) {
  // Instance 2 newest for all DBs

  auto const default_db_uuid = std::string{UUID()};
  auto const db_a = std::string{UUID()};
  auto const db_b = std::string{UUID()};

  std::map<std::string, InstanceInfo> instances_info{
      {"instance_1",
       InstanceInfo{.dbs_info = std::vector{InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 11},
                                            InstanceDBInfo{.db_uuid = db_a, .num_committed_txns = 51},
                                            InstanceDBInfo{.db_uuid = db_b, .num_committed_txns = 30}},
                    .last_committed_system_timestamp = 1}},
      {"instance_2", InstanceInfo{.dbs_info =
                                      std::vector{
                                          InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 15},
                                          InstanceDBInfo{.db_uuid = db_a, .num_committed_txns = 57},
                                          InstanceDBInfo{.db_uuid = db_b, .num_committed_txns = 33},
                                      },
                                  .last_committed_system_timestamp = 1

                     }},
      {"instance_3", InstanceInfo{.dbs_info =
                                      std::vector{
                                          InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 12},
                                          InstanceDBInfo{.db_uuid = db_a, .num_committed_txns = 56},
                                          InstanceDBInfo{.db_uuid = db_b, .num_committed_txns = 31},
                                      },
                                  .last_committed_system_timestamp = 1}},

  };

  auto const maybe_instance_name = CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_TRUE(*maybe_instance_name == "instance_2");
}

TEST(CoordinationUtils, MTFailover2) {
  // Instance 3 newest for 2 DBs

  auto const default_db_uuid = std::string{UUID()};
  auto const db_a = std::string{UUID()};
  auto const db_b = std::string{UUID()};

  std::map<std::string, InstanceInfo> instances_info{
      {"instance_1",
       InstanceInfo{.dbs_info = std::vector{InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 11},
                                            InstanceDBInfo{.db_uuid = db_a, .num_committed_txns = 51},
                                            InstanceDBInfo{.db_uuid = db_b, .num_committed_txns = 30}},
                    .last_committed_system_timestamp = 1}},
      {"instance_2", InstanceInfo{.dbs_info =
                                      std::vector{
                                          InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 15},
                                          InstanceDBInfo{.db_uuid = db_a, .num_committed_txns = 57},
                                          InstanceDBInfo{.db_uuid = db_b, .num_committed_txns = 33},
                                      },
                                  .last_committed_system_timestamp = 1

                     }},
      {"instance_3", InstanceInfo{.dbs_info =
                                      std::vector{
                                          InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 0},
                                          InstanceDBInfo{.db_uuid = db_a, .num_committed_txns = 58},
                                          InstanceDBInfo{.db_uuid = db_b, .num_committed_txns = 36},
                                      },
                                  .last_committed_system_timestamp = 1}},

  };

  auto const maybe_instance_name = CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_TRUE(*maybe_instance_name == "instance_3");
}

TEST(CoordinationUtils, MTFailover3) {
  // Instance 2 best for default db, best for db_a
  // Instance 1 best for default db
  // Instance 3 best for db_b

  auto const default_db_uuid = std::string{UUID()};
  auto const db_a = std::string{UUID()};
  auto const db_b = std::string{UUID()};

  std::map<std::string, InstanceInfo> instances_info{
      {"instance_1",
       InstanceInfo{.dbs_info = std::vector{InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 15},
                                            InstanceDBInfo{.db_uuid = db_a, .num_committed_txns = 51},
                                            InstanceDBInfo{.db_uuid = db_b, .num_committed_txns = 30}},
                    .last_committed_system_timestamp = 1}},
      {"instance_2", InstanceInfo{.dbs_info =
                                      std::vector{
                                          InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 15},
                                          InstanceDBInfo{.db_uuid = db_a, .num_committed_txns = 57},
                                          InstanceDBInfo{.db_uuid = db_b, .num_committed_txns = 33},
                                      },
                                  .last_committed_system_timestamp = 1

                     }},
      {"instance_3", InstanceInfo{.dbs_info =
                                      std::vector{
                                          InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 0},
                                          InstanceDBInfo{.db_uuid = db_a, .num_committed_txns = 55},
                                          InstanceDBInfo{.db_uuid = db_b, .num_committed_txns = 34},
                                      },
                                  .last_committed_system_timestamp = 1}},

  };

  auto const maybe_instance_name = CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_TRUE(*maybe_instance_name == "instance_2");
}

TEST(CoordinationUtils, MTFailover4) {
  // Instance 1 best for all 3 DBs
  // Instance 2 and 3 for default_db_uuid and db_a

  auto const default_db_uuid = std::string{UUID()};
  auto const db_a = std::string{UUID()};
  auto const db_b = std::string{UUID()};

  std::map<std::string, InstanceInfo> instances_info{
      {"instance_1",
       InstanceInfo{.dbs_info = std::vector{InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 15},
                                            InstanceDBInfo{.db_uuid = db_a, .num_committed_txns = 51},
                                            InstanceDBInfo{.db_uuid = db_b, .num_committed_txns = 30}},
                    .last_committed_system_timestamp = 1}},
      {"instance_2", InstanceInfo{.dbs_info =
                                      std::vector{
                                          InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 15},
                                          InstanceDBInfo{.db_uuid = db_a, .num_committed_txns = 51},
                                          InstanceDBInfo{.db_uuid = db_b, .num_committed_txns = 29},
                                      },
                                  .last_committed_system_timestamp = 1

                     }},
      {"instance_3", InstanceInfo{.dbs_info =
                                      std::vector{
                                          InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 15},
                                          InstanceDBInfo{.db_uuid = db_a, .num_committed_txns = 51},
                                          InstanceDBInfo{.db_uuid = db_b, .num_committed_txns = 28},
                                      },
                                  .last_committed_system_timestamp = 1}},

  };

  auto const maybe_instance_name =
      memgraph::coordination::CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_TRUE(*maybe_instance_name == "instance_1");
}

TEST(CoordinationUtils, MTFailover5) {
  // The decision needs to be done by summing timestamps
  // All 3 instances are up to date on db_b
  // Instance 1 is the best on default_db_uuid and has +5 over instance_2
  // Instance 2 is the best on db_a and has +6 over instance 1 -> therefore the winner

  auto const default_db_uuid = std::string{UUID()};
  auto const db_a = std::string{UUID()};
  auto const db_b = std::string{UUID()};

  std::map<std::string, InstanceInfo> instances_info{
      {"instance_1",
       InstanceInfo{.dbs_info = std::vector{InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 15},
                                            InstanceDBInfo{.db_uuid = db_a, .num_committed_txns = 19},
                                            InstanceDBInfo{.db_uuid = db_b, .num_committed_txns = 30}},
                    .last_committed_system_timestamp = 1}},
      {"instance_2", InstanceInfo{.dbs_info =
                                      std::vector{
                                          InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 10},
                                          InstanceDBInfo{.db_uuid = db_a, .num_committed_txns = 25},
                                          InstanceDBInfo{.db_uuid = db_b, .num_committed_txns = 30},
                                      },
                                  .last_committed_system_timestamp = 1

                     }},
      {"instance_3", InstanceInfo{.dbs_info =
                                      std::vector{
                                          InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 8},
                                          InstanceDBInfo{.db_uuid = db_a, .num_committed_txns = 8},
                                          InstanceDBInfo{.db_uuid = db_b, .num_committed_txns = 30},
                                      },
                                  .last_committed_system_timestamp = 1}},

  };

  auto const maybe_instance_name =
      memgraph::coordination::CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_TRUE(*maybe_instance_name == "instance_2");
}

TEST(CoordinationUtils, MTFailover6) {
  // The decision needs to be done by summing timestamps
  // Instance 1 is the best on default_db_uuid and has +6 over instance_1
  // Instance 2 is the best on db_a and has +5 over instance 1
  // --> Instance 1 is the winner

  auto const default_db_uuid = std::string{UUID()};
  auto const db_a = std::string{UUID()};

  std::map<std::string, InstanceInfo> instances_info{
      {"instance_1",
       InstanceInfo{.dbs_info = std::vector{InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 16},
                                            InstanceDBInfo{.db_uuid = db_a, .num_committed_txns = 19}},
                    .last_committed_system_timestamp = 1}},
      {"instance_2", InstanceInfo{.dbs_info =
                                      std::vector{
                                          InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 10},
                                          InstanceDBInfo{.db_uuid = db_a, .num_committed_txns = 24},
                                      },
                                  .last_committed_system_timestamp = 1}},
  };

  auto const maybe_instance_name =
      memgraph::coordination::CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_TRUE(*maybe_instance_name == "instance_1");
}

TEST(CoordinationUtils, MTFailover7) {
  // Instances are equally good on the same number of instances and they have the same total sum of timestamps.
  // Take the 1st one as winner

  auto const default_db_uuid = std::string{memgraph::utils::UUID()};
  auto const db_a = std::string{memgraph::utils::UUID()};

  std::map<std::string, InstanceInfo> instances_info{
      {"instance_1",
       InstanceInfo{.dbs_info = std::vector{InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 16},
                                            InstanceDBInfo{.db_uuid = db_a, .num_committed_txns = 10}},
                    .last_committed_system_timestamp = 1}},
      {"instance_2", InstanceInfo{.dbs_info =
                                      std::vector{
                                          InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 10},
                                          InstanceDBInfo{.db_uuid = db_a, .num_committed_txns = 16},
                                      },
                                  .last_committed_system_timestamp = 1}},
  };

  auto const maybe_instance_name =
      memgraph::coordination::CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_TRUE(*maybe_instance_name == "instance_1");
}

TEST(CoordinationUtils, MTFailover8) {
  // Both instances are equally good on default DB.
  // Instance has larger sys timstamp so db_b and default_db_uuid are the relevant ones

  auto const default_db_uuid = std::string{memgraph::utils::UUID()};
  auto const db_a = std::string{memgraph::utils::UUID()};
  auto const db_b = std::string{memgraph::utils::UUID()};

  std::map<std::string, InstanceInfo> instances_info{
      {"instance_1", InstanceInfo{.dbs_info =
                                      std::vector{
                                          InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 16},
                                          InstanceDBInfo{.db_uuid = db_a, .num_committed_txns = 20},

                                      },
                                  .last_committed_system_timestamp = 1}},
      {"instance_2", InstanceInfo{.dbs_info =
                                      std::vector{
                                          InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 16},
                                          InstanceDBInfo{.db_uuid = db_b, .num_committed_txns = 20},
                                      },
                                  .last_committed_system_timestamp = 2}},
  };

  auto const maybe_instance_name =
      memgraph::coordination::CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_TRUE(*maybe_instance_name == "instance_2");
}

TEST(CoordinationUtils, MTFailover9) {
  // Only take into account default DB for instance_2 on which it is better

  auto const default_db_uuid = std::string{memgraph::utils::UUID()};
  auto const db_a = std::string{memgraph::utils::UUID()};

  std::map<std::string, InstanceInfo> instances_info{
      {"instance_1", InstanceInfo{.dbs_info =
                                      std::vector{
                                          InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 16},
                                          InstanceDBInfo{.db_uuid = db_a, .num_committed_txns = 20},

                                      },
                                  .last_committed_system_timestamp = 1}},
      {"instance_2", InstanceInfo{.dbs_info =
                                      std::vector{
                                          InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 17},
                                      },
                                  .last_committed_system_timestamp = 2}},
  };

  auto const maybe_instance_name =
      memgraph::coordination::CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_TRUE(*maybe_instance_name == "instance_2");
}

TEST(CoordinationUtils, FailoverNoInstancesAvailable) {
  std::map<std::string, InstanceInfo> const instances_info{};

  auto const maybe_instance_name =
      memgraph::coordination::CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_FALSE(maybe_instance_name.has_value());
}

TEST(CoordinationUtils, FailoverSomeInstancesMissingNumTxns) {
  // missing timestamps
  auto const default_db_uuid = std::string{memgraph::utils::UUID()};

  std::map<std::string, InstanceInfo> const instances_info{
      {"instance_1",
       InstanceInfo{.dbs_info = std::vector{InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 0}},
                    .last_committed_system_timestamp = 1}},
      {"instance_2",
       InstanceInfo{.dbs_info = std::vector{InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 15}},
                    .last_committed_system_timestamp = 1}},
      {"instance_3",
       InstanceInfo{.dbs_info = std::vector{InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 0}},
                    .last_committed_system_timestamp = 1}},

  };

  auto const maybe_instance_name =
      memgraph::coordination::CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_EQ(*maybe_instance_name, "instance_2");
}

TEST(CoordinationUtils, FailoverSingleInstanceOnly) {
  auto const default_db_uuid = std::string{memgraph::utils::UUID()};

  std::map<std::string, InstanceInfo> const instances_info{
      {"instance_1",
       InstanceInfo{.dbs_info = std::vector{InstanceDBInfo{.db_uuid = default_db_uuid, .num_committed_txns = 100}},
                    .last_committed_system_timestamp = 1}},
  };

  auto const maybe_instance_name =
      memgraph::coordination::CoordinatorInstance::ChooseMostUpToDateInstance(instances_info);
  ASSERT_TRUE(maybe_instance_name.has_value());
  // NOLINTNEXTLINE
  ASSERT_EQ(*maybe_instance_name, "instance_1");
}
