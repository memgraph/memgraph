#pragma once

#include <map>

#include "glog/logging.h"

#include "communication/bolt/v1/decoder/decoded_value.hpp"
#include "utils/algorithm.hpp"

// TODO: Why is this here?! It's only used in tests and query/repl.cpp

/**
 * A mocker for the data output record stream.
 * This implementation checks that messages are
 * sent to it in an acceptable order, and tracks
 * the content of those messages.
 */
template <class TResultValue = communication::bolt::DecodedValue>
class ResultStreamFaker {
 public:
  ResultStreamFaker() = default;
  ResultStreamFaker(const ResultStreamFaker &) = delete;
  ResultStreamFaker &operator=(const ResultStreamFaker &) = delete;
  ResultStreamFaker(ResultStreamFaker &&) = default;
  ResultStreamFaker &operator=(ResultStreamFaker &&) = default;

  void Header(const std::vector<std::string> &fields) {
    DCHECK(current_state_ == State::Start)
        << "Headers can only be written in the beginning";
    header_ = fields;
    current_state_ = State::WritingResults;
  }

  void Result(const std::vector<TResultValue> &values) {
    DCHECK(current_state_ == State::WritingResults)
        << "Can't accept results before header nor after summary";
    results_.push_back(values);
  }

  void Summary(
      const std::map<std::string, communication::bolt::DecodedValue> &summary) {
    DCHECK(current_state_ != State::Done) << "Can only send a summary once";
    summary_ = summary;
    current_state_ = State::Done;
  }

  const auto &GetHeader() const {
    DCHECK(current_state_ != State::Start) << "Header not written";
    return header_;
  }

  const auto &GetResults() const { return results_; }

  const auto &GetSummary() const {
    DCHECK(current_state_ == State::Done) << "Summary not written";
    return summary_;
  }

  friend std::ostream &operator<<(std::ostream &os,
                                  const ResultStreamFaker &results) {
    auto decoded_value_to_string = [](const auto &value) {
      std::stringstream ss;
      ss << value;
      return ss.str();
    };
    const std::vector<std::string> &header = results.GetHeader();
    std::vector<int> column_widths(header.size());
    std::transform(header.begin(), header.end(), column_widths.begin(),
                   [](const auto &s) { return s.size(); });

    // convert all the results into strings, and track max column width
    auto &results_data = results.GetResults();
    std::vector<std::vector<std::string>> result_strings(
        results_data.size(), std::vector<std::string>(column_widths.size()));
    for (int row_ind = 0; row_ind < static_cast<int>(results_data.size());
         ++row_ind) {
      for (int col_ind = 0; col_ind < static_cast<int>(column_widths.size());
           ++col_ind) {
        std::string string_val =
            decoded_value_to_string(results_data[row_ind][col_ind]);
        column_widths[col_ind] =
            std::max(column_widths[col_ind], (int)string_val.size());
        result_strings[row_ind][col_ind] = string_val;
      }
    }

    // output a results table
    // first define some helper functions
    auto emit_horizontal_line = [&]() {
      os << "+";
      for (auto col_width : column_widths)
        os << std::string((unsigned long)col_width + 2, '-') << "+";
      os << std::endl;
    };

    auto emit_result_vec = [&](const std::vector<std::string> result_vec) {
      os << "| ";
      for (int col_ind = 0; col_ind < static_cast<int>(column_widths.size());
           ++col_ind) {
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
    utils::PrintIterable(os, results.GetSummary(), ", ",
                         [&](auto &stream, const auto &kv) {
                           stream << kv.first << ": " << kv.second;
                         });
    os << "}" << std::endl;

    return os;
  }

 private:
  /**
   * Possible states of the Mocker. Used for checking if calls to
   * the Mocker as in acceptable order.
   */
  enum class State { Start, WritingResults, Done };

  // the current state
  State current_state_ = State::Start;

  // the data that the record stream can accept
  std::vector<std::string> header_;
  std::vector<std::vector<TResultValue>> results_;
  std::map<std::string, communication::bolt::DecodedValue> summary_;
};
