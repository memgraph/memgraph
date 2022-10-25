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

#include "query/v2/plan/profile.hpp"

#include <algorithm>
#include <chrono>

#include <fmt/format.h>
#include <json/json.hpp>

#include "query/v2/context.hpp"
#include "utils/likely.hpp"

namespace memgraph::query::v2::plan {

namespace {

uint64_t IndividualCycles(const ProfilingStats &cumulative_stats) {
  return cumulative_stats.num_cycles - std::accumulate(cumulative_stats.children.begin(),
                                                       cumulative_stats.children.end(), 0ULL,
                                                       [](auto acc, auto &stats) { return acc + stats.num_cycles; });
}

double RelativeTime(uint64_t num_cycles, uint64_t total_cycles) {
  return static_cast<double>(num_cycles) / static_cast<double>(total_cycles);
}

double AbsoluteTime(uint64_t num_cycles, uint64_t total_cycles, std::chrono::duration<double> total_time) {
  return (RelativeTime(num_cycles, total_cycles) * static_cast<std::chrono::duration<double, std::milli>>(total_time))
      .count();
}

}  // namespace

//////////////////////////////////////////////////////////////////////////////
//
// ProfilingStatsToTable

namespace {

class ProfilingStatsToTableHelper {
 public:
  ProfilingStatsToTableHelper(uint64_t total_cycles, std::chrono::duration<double> total_time)
      : total_cycles_(total_cycles), total_time_(total_time) {}

  void Output(const ProfilingStats &cumulative_stats) {
    auto cycles = IndividualCycles(cumulative_stats);
    auto custom_data_copy = cumulative_stats.custom_data;
    ConvertCyclesToTime(custom_data_copy);

    rows_.emplace_back(
        std::vector<TypedValue>{TypedValue(FormatOperator(cumulative_stats.name)),
                                TypedValue(cumulative_stats.actual_hits), TypedValue(FormatRelativeTime(cycles)),
                                TypedValue(FormatAbsoluteTime(cycles)), TypedValue(custom_data_copy.dump())});

    for (size_t i = 1; i < cumulative_stats.children.size(); ++i) {
      Branch(cumulative_stats.children[i]);
    }

    if (!cumulative_stats.children.empty()) {
      Output(cumulative_stats.children[0]);
    }
  }

  std::vector<std::vector<TypedValue>> rows() const { return rows_; }

 private:
  void Branch(const ProfilingStats &cumulative_stats) {
    rows_.emplace_back(std::vector<TypedValue>{TypedValue("|\\"), TypedValue(""), TypedValue(""), TypedValue("")});

    ++depth_;
    Output(cumulative_stats);
    --depth_;
  }

  double AbsoluteTime(const uint64_t cycles) const { return plan::AbsoluteTime(cycles, total_cycles_, total_time_); }

  double RelativeTime(const uint64_t cycles) const { return plan::RelativeTime(cycles, total_cycles_); }

  void ConvertCyclesToTime(nlohmann::json &custom_data) const {
    const auto convert_cycles_in_json = [this](nlohmann::json &json) {
      if (!json.is_object()) {
        return;
      }
      if (auto it = json.find(ProfilingStats::kNumCycles); it != json.end()) {
        auto num_cycles = it.value().get<uint64_t>();
        json[ProfilingStats::kAbsoluteTime] = AbsoluteTime(num_cycles);
        json[ProfilingStats::kRelativeTime] = RelativeTime(num_cycles);
      }
    };

    for (auto &json : custom_data) {
      convert_cycles_in_json(json);
    }
  }

  std::string Format(const char *str) const {
    std::ostringstream ss;
    for (int64_t i = 0; i < depth_; ++i) {
      ss << "| ";
    }
    ss << str;
    return ss.str();
  }

  std::string Format(const std::string &str) const { return Format(str.c_str()); }

  std::string FormatOperator(const char *str) const { return Format(std::string("* ") + str); }

  std::string FormatRelativeTime(uint64_t num_cycles) const {
    return fmt::format("{: 10.6f} %", RelativeTime(num_cycles) * 100);
  }

  std::string FormatAbsoluteTime(uint64_t num_cycles) const {
    return fmt::format("{: 10.6f} ms", AbsoluteTime(num_cycles));
  }

  int64_t depth_{0};
  std::vector<std::vector<TypedValue>> rows_;
  uint64_t total_cycles_;
  std::chrono::duration<double> total_time_;
};

}  // namespace

std::vector<std::vector<TypedValue>> ProfilingStatsToTable(const ProfilingStatsWithTotalTime &stats) {
  ProfilingStatsToTableHelper helper{stats.cumulative_stats.num_cycles, stats.total_time};
  helper.Output(stats.cumulative_stats);
  return helper.rows();
}

//////////////////////////////////////////////////////////////////////////////
//
// ProfilingStatsToJson

namespace {

class ProfilingStatsToJsonHelper {
 private:
  using json = nlohmann::json;

 public:
  ProfilingStatsToJsonHelper(uint64_t total_cycles, std::chrono::duration<double> total_time)
      : total_cycles_(total_cycles), total_time_(total_time) {}

  void Output(const ProfilingStats &cumulative_stats) { return Output(cumulative_stats, &json_); }

  json ToJson() { return json_; }

 private:
  void Output(const ProfilingStats &cumulative_stats, json *obj) {
    auto cycles = IndividualCycles(cumulative_stats);

    obj->emplace("name", cumulative_stats.name);
    obj->emplace("actual_hits", cumulative_stats.actual_hits);
    obj->emplace("relative_time", RelativeTime(cycles, total_cycles_));
    obj->emplace("absolute_time", AbsoluteTime(cycles, total_cycles_, total_time_));
    obj->emplace("children", json::array());

    for (size_t i = 0; i < cumulative_stats.children.size(); ++i) {
      json child;
      Output(cumulative_stats.children[i], &child);
      obj->at("children").emplace_back(std::move(child));
    }
  }

  json json_;
  uint64_t total_cycles_;
  std::chrono::duration<double> total_time_;
};

}  // namespace

nlohmann::json ProfilingStatsToJson(const ProfilingStatsWithTotalTime &stats) {
  ProfilingStatsToJsonHelper helper{stats.cumulative_stats.num_cycles, stats.total_time};
  helper.Output(stats.cumulative_stats);
  return helper.ToJson();
}

}  // namespace memgraph::query::v2::plan
