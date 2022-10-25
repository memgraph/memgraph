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

#pragma once

#include <cstdint>

#include <json/json.hpp>

#include "query/v2/context.hpp"
#include "query/v2/plan/profile.hpp"
#include "utils/likely.hpp"
#include "utils/tsc.hpp"

namespace memgraph::query::v2::plan {

class ScopedCustomProfile {
 public:
  explicit ScopedCustomProfile(const std::string_view custom_data_name, ExecutionContext &context)
      : custom_data_name_(custom_data_name), start_time_{utils::ReadTSC()}, context_{&context} {}

  ScopedCustomProfile(const ScopedCustomProfile &) = delete;
  ScopedCustomProfile(ScopedCustomProfile &&) = delete;
  ScopedCustomProfile &operator=(const ScopedCustomProfile &) = delete;
  ScopedCustomProfile &operator=(ScopedCustomProfile &&) = delete;

  ~ScopedCustomProfile() {
    if (nullptr != context_->stats_root) {
      auto &custom_data = context_->stats_root->custom_data[custom_data_name_];
      if (!custom_data.is_object()) {
        custom_data = nlohmann::json::object();
      }
      const auto elapsed = utils::ReadTSC() - start_time_;
      auto &num_cycles_json = custom_data[ProfilingStats::kNumCycles];
      const auto num_cycles = num_cycles_json.is_null() ? 0 : num_cycles_json.get<uint64_t>();
      num_cycles_json = num_cycles + elapsed;
    }
  }

 private:
  std::string_view custom_data_name_;
  uint64_t start_time_;
  ExecutionContext *context_;
};

/**
 * A RAII class used for profiling logical operators. Instances of this class
 * update the profiling data stored within the `ExecutionContext` object and build
 * up a tree of `ProfilingStats` instances. The structure of the `ProfilingStats`
 * tree depends on the `LogicalOperator`s that were executed.
 */
class ScopedProfile {
 public:
  ScopedProfile(uint64_t key, const char *name, query::v2::ExecutionContext *context) noexcept : context_(context) {
    if (UNLIKELY(IsProfiling())) {
      root_ = context_->stats_root;

      // Are we the root logical operator?
      if (!root_) {
        stats_ = &context_->stats;
        stats_->key = key;
        stats_->name = name;
      } else {
        stats_ = nullptr;

        // Was this logical operator already hit on one of the previous pulls?
        auto it = std::find_if(root_->children.begin(), root_->children.end(),
                               [key](auto &stats) { return stats.key == key; });

        if (it == root_->children.end()) {
          root_->children.emplace_back();
          stats_ = &root_->children.back();
          stats_->key = key;
          stats_->name = name;
        } else {
          stats_ = &(*it);
        }
      }

      context_->stats_root = stats_;
      stats_->actual_hits++;
      start_time_ = utils::ReadTSC();
    }
  }

  ScopedProfile(const ScopedProfile &) = delete;
  ScopedProfile(ScopedProfile &&) = delete;
  ScopedProfile &operator=(const ScopedProfile &) = delete;
  ScopedProfile &operator=(ScopedProfile &&) = delete;

  ~ScopedProfile() noexcept {
    if (UNLIKELY(IsProfiling())) {
      stats_->num_cycles += utils::ReadTSC() - start_time_;

      // Restore the old root ("pop")
      context_->stats_root = root_;
    }
  }

 private:
  [[nodiscard]] bool IsProfiling() const { return context_->is_profile_query; }

  query::v2::ExecutionContext *context_;
  ProfilingStats *root_{nullptr};
  ProfilingStats *stats_{nullptr};
  uint64_t start_time_{0};
};

}  // namespace memgraph::query::v2::plan
