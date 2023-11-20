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

#include "query/plan/profile.hpp"

#include <algorithm>
#include <chrono>

#include <fmt/format.h>
#include <json/json.hpp>

#include "query/context.hpp"
#include "utils/likely.hpp"

namespace memgraph::query::plan {

namespace {

unsigned long long IndividualCycles(const ProfilingStats &cumulative_stats) {
  return cumulative_stats.num_cycles - std::accumulate(cumulative_stats.children.begin(),
                                                       cumulative_stats.children.end(), 0ULL,
                                                       [](auto acc, auto &stats) { return acc + stats.num_cycles; });
}

double RelativeTime(unsigned long long num_cycles, unsigned long long total_cycles) {
  return static_cast<double>(num_cycles) / total_cycles;
}

double AbsoluteTime(unsigned long long num_cycles, unsigned long long total_cycles,
                    std::chrono::duration<double> total_time) {
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
  ProfilingStatsToTableHelper(unsigned long long total_cycles, std::chrono::duration<double> total_time)
      : total_cycles_(total_cycles), total_time_(total_time) {}

  void Output(const ProfilingStats &cumulative_stats) {
    auto cycles = IndividualCycles(cumulative_stats);

    rows_.emplace_back(std::vector<TypedValue>{
        TypedValue(FormatOperator(cumulative_stats.name.c_str())), TypedValue(cumulative_stats.actual_hits),
        TypedValue(FormatRelativeTime(cycles)), TypedValue(FormatAbsoluteTime(cycles))});

    for (size_t i = 1; i < cumulative_stats.children.size(); ++i) {
      Branch(cumulative_stats.children[i]);
    }

    if (cumulative_stats.children.size() >= 1) {
      Output(cumulative_stats.children[0]);
    }
  }

  std::vector<std::vector<TypedValue>> rows() { return rows_; }

 private:
  void Branch(const ProfilingStats &cumulative_stats) {
    rows_.emplace_back(std::vector<TypedValue>{TypedValue("|\\"), TypedValue(""), TypedValue(""), TypedValue("")});

    ++depth_;
    Output(cumulative_stats);
    --depth_;
  }

  std::string Format(const char *str) {
    std::ostringstream ss;
    for (int64_t i = 0; i < depth_; ++i) {
      ss << "| ";
    }
    ss << str;
    return ss.str();
  }

  std::string Format(const std::string &str) { return Format(str.c_str()); }

  std::string FormatOperator(const char *str) { return Format(std::string("* ") + str); }

  std::string FormatRelativeTime(unsigned long long num_cycles) {
    return fmt::format("{: 10.6f} %", RelativeTime(num_cycles, total_cycles_) * 100);
  }

  std::string FormatAbsoluteTime(unsigned long long num_cycles) {
    return fmt::format("{: 10.6f} ms", AbsoluteTime(num_cycles, total_cycles_, total_time_));
  }

  int64_t depth_{0};
  std::vector<std::vector<TypedValue>> rows_;
  unsigned long long total_cycles_;
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
  ProfilingStatsToJsonHelper(unsigned long long total_cycles, std::chrono::duration<double> total_time)
      : total_cycles_(total_cycles), total_time_(total_time) {}

  void Output(const ProfilingStats &cumulative_stats) { return Output(cumulative_stats, &json_); }

  json ToJson() { return json_; }

 private:
  void Output(const ProfilingStats &cumulative_stats, json *obj) {
    auto cycles = IndividualCycles(cumulative_stats);

    obj->emplace("name", cumulative_stats.name.c_str());
    obj->emplace("actual_hits", cumulative_stats.actual_hits);
    obj->emplace("relative_time", RelativeTime(cycles, total_cycles_));
    obj->emplace("absolute_time", AbsoluteTime(cycles, total_cycles_, total_time_));
    obj->emplace("children", json::array());

    for (const auto &child : cumulative_stats.children) {
      json json_child;
      Output(child, &json_child);
      obj->at("children").emplace_back(std::move(json_child));
    }
  }

  json json_;
  unsigned long long total_cycles_;
  std::chrono::duration<double> total_time_;
};

}  // namespace

nlohmann::json ProfilingStatsToJson(const ProfilingStatsWithTotalTime &stats) {
  ProfilingStatsToJsonHelper helper{stats.cumulative_stats.num_cycles, stats.total_time};
  helper.Output(stats.cumulative_stats);
  return helper.ToJson();
}

}  // namespace memgraph::query::plan
