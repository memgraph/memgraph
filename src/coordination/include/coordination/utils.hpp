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
#pragma once

#include <optional>
#include <string>
#include <unordered_map>

#include "dbms/constants.hpp"
#include "replication_coordination_glue/common.hpp"

namespace memgraph::coordination {

inline std::string ChooseMostUpToDateInstance(
    const std::vector<std::pair<std::string, replication_coordination_glue::DatabaseHistories>>
        &instance_database_histories,
    std::optional<std::string> &latest_epoch, std::optional<uint64_t> &latest_commit_timestamp) {
  std::string most_up_to_date_instance;
  // TODO isolate into function
  std::for_each(
      instance_database_histories.begin(), instance_database_histories.end(),
      [&latest_epoch, &latest_commit_timestamp, &most_up_to_date_instance](
          const std::pair<const std::string, replication_coordination_glue::DatabaseHistories> &instance_res_pair) {
        const auto &[instance_name, instance_db_histories] = instance_res_pair;

        // Find default db for instance and its history
        auto default_db_history_data =
            std::find_if(instance_db_histories.begin(), instance_db_histories.end(),
                         [default_db = memgraph::dbms::kDefaultDB](
                             const replication_coordination_glue::DatabaseHistory &db_timestamps) {
                           return db_timestamps.name == default_db;
                         });

        // TODO remove only for logging purposes
        std::for_each(
            instance_db_histories.begin(), instance_db_histories.end(),
            [instance_name = instance_name](const replication_coordination_glue::DatabaseHistory &db_history) {
              spdlog::trace("Instance {}: name {}, default db {}", instance_name, db_history.name,
                            memgraph::dbms::kDefaultDB);
            });

        // auto error_msg = std::string(fmt::format("No history for instance {}", instance_name));
        MG_ASSERT(default_db_history_data != instance_db_histories.end(), "No history for instance");

        const auto &instance_default_db_history = default_db_history_data->history;

        // TODO remove only for logging purposes
        std::for_each(instance_default_db_history.rbegin(), instance_default_db_history.rend(),
                      [instance_name = instance_name](const auto &instance_default_db_history_it) {
                        spdlog::trace("Instance {}:  epoch {}, last_commit_timestamp: {}", instance_name,
                                      std::get<1>(instance_default_db_history_it),
                                      std::get<0>(instance_default_db_history_it));
                      });

        // get latest epoch
        // get latest timestamp

        // if current history is none, I am first one
        // if there is some kind history recorded, check that I am older
        if (!latest_epoch) {
          const auto it = instance_default_db_history.crbegin();
          const auto &[epoch, timestamp] = *it;
          latest_epoch.emplace(epoch);
          latest_commit_timestamp.emplace(timestamp);
          most_up_to_date_instance = instance_name;
          spdlog::trace("Currently the most up to date instance is {} with epoch {} and {} latest commit timestamp",
                        instance_name, epoch, timestamp);
          return;
        }

        for (auto it = instance_default_db_history.rbegin(); it != instance_default_db_history.rend(); ++it) {
          const auto &[epoch, timestamp] = *it;

          // we found point at which they were same
          if (epoch == *latest_epoch) {
            // if this is the latest history, compare timestamps
            if (it == instance_default_db_history.rbegin()) {
              if (*latest_commit_timestamp < timestamp) {
                latest_commit_timestamp.emplace(timestamp);
                most_up_to_date_instance = instance_name;
                spdlog::trace("Found new the most up to date instance {} with epoch {} and {} latest commit timestamp",
                              instance_name, epoch, timestamp);
              }
            } else {
              latest_epoch.emplace(instance_default_db_history.rbegin()->first);
              latest_commit_timestamp.emplace(instance_default_db_history.rbegin()->second);
              most_up_to_date_instance = instance_name;
              spdlog::trace("Found new the most up to date instance {} with epoch {} and {} latest commit timestamp",
                            instance_name, epoch, timestamp);
            }
            break;
          }
          // else if we don't find epoch which is same, instance with current most_up_to_date_instance
          // is ahead
        }
      });

  return most_up_to_date_instance;
}

}  // namespace memgraph::coordination
