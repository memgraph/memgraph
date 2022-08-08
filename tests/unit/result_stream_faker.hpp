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

#include <map>

#include "glue/v2/communication.hpp"
#include "query/v2/bindings/typed_value.hpp"

#include "storage/v3/storage.hpp"
#include "utils/algorithm.hpp"

/**
 * A mocker for the data output record stream.
 * This implementation checks that messages are
 * sent to it in an acceptable order, and tracks
 * the content of those messages.
 */
class ResultStreamFaker {
 public:
  explicit ResultStreamFaker(memgraph::storage::v3::Storage *store) : store_(store) {}

  ResultStreamFaker(const ResultStreamFaker &) = delete;
  ResultStreamFaker &operator=(const ResultStreamFaker &) = delete;
  ResultStreamFaker(ResultStreamFaker &&) = default;
  ResultStreamFaker &operator=(ResultStreamFaker &&) = default;

  void Header(const std::vector<std::string> &fields) { header_ = fields; }

  void Result(const std::vector<memgraph::communication::bolt::Value> &values) { results_.push_back(values); }

  void Result(const std::vector<memgraph::query::v2::TypedValue> &values) {
    std::vector<memgraph::communication::bolt::Value> bvalues;
    bvalues.reserve(values.size());
    for (const auto &value : values) {
      auto maybe_value = memgraph::glue::v2::ToBoltValue(value, *store_, memgraph::storage::v3::View::NEW);
      MG_ASSERT(maybe_value.HasValue());
      bvalues.push_back(std::move(*maybe_value));
    }
    results_.push_back(std::move(bvalues));
  }

  void Summary(const std::map<std::string, memgraph::communication::bolt::Value> &summary) { summary_ = summary; }

  void Summary(const std::map<std::string, memgraph::query::v2::TypedValue> &summary) {
    std::map<std::string, memgraph::communication::bolt::Value> bsummary;
    for (const auto &item : summary) {
      auto maybe_value = memgraph::glue::v2::ToBoltValue(item.second, *store_, memgraph::storage::v3::View::NEW);
      MG_ASSERT(maybe_value.HasValue());
      bsummary.insert({item.first, std::move(*maybe_value)});
    }
    summary_ = std::move(bsummary);
  }

  const auto &GetHeader() const { return header_; }

  const auto &GetResults() const { return results_; }

  const auto &GetSummary() const { return summary_; }

  friend std::ostream &operator<<(std::ostream &os, const ResultStreamFaker &results) {
    auto decoded_value_to_string = [](const auto &value) {
      std::stringstream ss;
      ss << value;
      return ss.str();
    };
    const std::vector<std::string> &header = results.GetHeader();
    std::vector<int> column_widths(header.size());
    std::transform(header.begin(), header.end(), column_widths.begin(), [](const auto &s) { return s.size(); });

    // convert all the results into strings, and track max column width
    auto &results_data = results.GetResults();
    std::vector<std::vector<std::string>> result_strings(results_data.size(),
                                                         std::vector<std::string>(column_widths.size()));
    for (int row_ind = 0; row_ind < static_cast<int>(results_data.size()); ++row_ind) {
      for (int col_ind = 0; col_ind < static_cast<int>(column_widths.size()); ++col_ind) {
        std::string string_val = decoded_value_to_string(results_data[row_ind][col_ind]);
        column_widths[col_ind] = std::max(column_widths[col_ind], (int)string_val.size());
        result_strings[row_ind][col_ind] = string_val;
      }
    }

    // output a results table
    // first define some helper functions
    auto emit_horizontal_line = [&]() {
      os << "+";
      for (auto col_width : column_widths) os << std::string((unsigned long)col_width + 2, '-') << "+";
      os << std::endl;
    };

    auto emit_result_vec = [&](const std::vector<std::string> result_vec) {
      os << "| ";
      for (int col_ind = 0; col_ind < static_cast<int>(column_widths.size()); ++col_ind) {
        const std::string &res = result_vec[col_ind];
        os << res << std::string(column_widths[col_ind] - res.size(), ' ');
        os << " | ";
      }
      os << std::endl;
    };

    // final output of results
    emit_horizontal_line();
    emit_result_vec(results.GetHeader());
    emit_horizontal_line();
    for (const auto &result_vec : result_strings) emit_result_vec(result_vec);
    emit_horizontal_line();
    os << "Found " << results_data.size() << " matching results" << std::endl;

    // output the summary
    os << "Query summary: {";
    memgraph::utils::PrintIterable(os, results.GetSummary(), ", ",
                                   [&](auto &stream, const auto &kv) { stream << kv.first << ": " << kv.second; });
    os << "}" << std::endl;

    return os;
  }

 private:
  memgraph::storage::v3::Storage *store_;
  // the data that the record stream can accept
  std::vector<std::string> header_;
  std::vector<std::vector<memgraph::communication::bolt::Value>> results_;
  std::map<std::string, memgraph::communication::bolt::Value> summary_;
};
