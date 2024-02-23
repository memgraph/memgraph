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
#include <range/v3/view.hpp>
#include <ranges>
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

        std::for_each(
            instance_db_histories.begin(), instance_db_histories.end(),
            [instance_name = instance_name](const replication_coordination_glue::DatabaseHistory &db_history) {
              spdlog::trace("Instance {}: name {}, default db {}", instance_name, db_history.name,
                            memgraph::dbms::kDefaultDB);
            });

        MG_ASSERT(default_db_history_data != instance_db_histories.end(), "No history for instance");

        const auto &instance_default_db_history = default_db_history_data->history;

        std::for_each(instance_default_db_history.rbegin(), instance_default_db_history.rend(),
                      [instance_name = instance_name](const auto &instance_default_db_history_it) {
                        spdlog::trace("Instance {}:  epoch {}, last_commit_timestamp: {}", instance_name,
                                      std::get<1>(instance_default_db_history_it),
                                      std::get<0>(instance_default_db_history_it));
                      });

        // get latest epoch
        // get latest timestamp

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

        bool found_same_point{false};
        std::string last_most_up_to_date_epoch{*latest_epoch};
        for (auto [epoch, timestamp] : ranges::reverse_view(instance_default_db_history)) {
          if (*latest_commit_timestamp < timestamp) {
            latest_commit_timestamp.emplace(timestamp);
            latest_epoch.emplace(epoch);
            most_up_to_date_instance = instance_name;
            spdlog::trace("Found the new most up to date instance {} with epoch {} and {} latest commit timestamp",
                          instance_name, epoch, timestamp);
          }

          // we found point at which they were same
          if (epoch == last_most_up_to_date_epoch) {
            found_same_point = true;
            break;
          }
        }

        if (!found_same_point) {
          spdlog::error("Didn't find same history epoch {} for instance {} and instance {}", last_most_up_to_date_epoch,
                        most_up_to_date_instance, instance_name);
        }
      });

  return most_up_to_date_instance;
}

}  // namespace memgraph::coordination
