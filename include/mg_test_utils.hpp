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

#pragma once

#include <cassert>
#include <chrono>
#include <cmath>
#include <set>
#include <stack>
#include <type_traits>
#include <unordered_map>
#include <vector>

namespace mg_test_utility {

constexpr double ABSOLUTE_ERROR_EPSILON{10e-4};
constexpr double AVERAGE_ABSOLUTE_ERROR_EPSILON{10e-5};

/// This class is threadsafe
class Timer {
 public:
  explicit Timer() : start_time_(std::chrono::steady_clock::now()) {}

  template <typename TDuration = std::chrono::duration<double>>
  TDuration Elapsed() const {
    return std::chrono::duration_cast<TDuration>(std::chrono::steady_clock::now() - start_time_);
  }

 private:
  std::chrono::steady_clock::time_point start_time_;
};

///
///@brief Method that calculates the maximum absolute error between vector components.
/// The vector type must be an arithmetic type. Vectors must have the same size.
///
///@param result Container that stores calculated values
///@param correct Container that stores accurate values
///@return maximum absolute error
///
template <typename T>
T MaxAbsoluteError(const std::vector<T> &result, const std::vector<T> &correct) {
  static_assert(std::is_arithmetic_v<T>,
                "mg_test_utility::MaxAbsoluteError expects the vector type to be an arithmetic type.\n");

  assert(result.size() == correct.size());

  auto size = correct.size();
  T max_absolute_error = 0;

  for (auto index = 0; index < size; index++) {
    T absolute_distance = std::abs(result[index] - correct[index]);
    max_absolute_error = std::max(max_absolute_error, absolute_distance);
  }

  return max_absolute_error;
}

///
///@brief Method that calculates the average absolute error between vector components.
/// The vector type must be an arithmetic type. Vectors must have the same size.
///
///@param result Container that stores calculated values
///@param correct Container that stores accurate values
///@return average absolute error
///
template <typename T>
double AverageAbsoluteError(const std::vector<T> &result, const std::vector<T> &correct) {
  static_assert(std::is_arithmetic_v<T>,
                "mg_test_utility::AverageAbsoluteError expects the vector type to be an arithmetic type.\n");

  assert(result.size() == correct.size());

  auto size = correct.size();
  T manhattan_distance = 0;

  for (auto index = 0; index < size; index++) {
    T absolute_distance = std::abs(result[index] - correct[index]);
    manhattan_distance += absolute_distance;
  }

  double average_absolute_error = 0;
  if (size > 0) average_absolute_error = static_cast<double>(manhattan_distance) / size;
  return average_absolute_error;
}

///
///@brief A method that determines whether given vectors are the same within the defined tolerance.
/// The vector type must be an arithmetic type. Vectors must have the same size.
///
///@param result Container that stores calculated values
///@param correct Container that stores accurate values
///@return true if the maximum absolute error is lesser than ABSOLUTE_ERROR_EPSILON
/// and the average absolute error is lesser than AVERAGE_ABSOLUTE_ERROR_EPSILON,
/// false otherwise
///
template <typename T>
bool TestEqualVectors(const std::vector<T> &result, const std::vector<T> &correct,
                      double absolute_error = ABSOLUTE_ERROR_EPSILON,
                      double average_error = AVERAGE_ABSOLUTE_ERROR_EPSILON) {
  static_assert(std::is_arithmetic<T>::value, "Not an arithmetic type!");
  return MaxAbsoluteError(result, correct) < absolute_error && AverageAbsoluteError(result, correct) < absolute_error;
}

///
///@brief A method that determines whether given unordered maps with (int, arithmetic_type) values are the same within
/// defined tolerance. Maps must have the same keys.
///
///@param result Container storing calculated results
///@param correct Container storing correct values
///@param absolute_error Absolute error between any two pairs of resulting and correct value
///@return true if the maximum absolute error is lesser than ABSOLUTE_ERROR_EPSILON, false otherwise
///
template <typename T>
bool TestEqualUnorderedMaps(const std::unordered_map<std::uint64_t, T> &result,
                            const std::unordered_map<std::uint64_t, T> &correct,
                            double absolute_error = ABSOLUTE_ERROR_EPSILON) {
  static_assert(std::is_arithmetic<T>::value, "Not an arithmetic type!");

  if (result.size() != correct.size()) return false;

  auto get_map_keys = [](const std::unordered_map<std::uint64_t, T> &map) -> std::set<std::uint64_t> {
    std::set<std::uint64_t> keys;
    for (const auto [k, v] : map) {
      keys.insert(k);
    }
    return keys;
  };
  auto keys_result = get_map_keys(result);
  auto keys_correct = get_map_keys(correct);

  if (keys_result != keys_correct) return false;

  return std::equal(keys_result.begin(), keys_result.end(), keys_correct.begin(),
                    [&result, &correct, &absolute_error](auto key_result, auto key_correct) {
                      return std::abs(result.at(key_result) - correct.at(key_correct)) <= absolute_error;
                    });
}

///
///@brief A method that determines whether given vectors with (int, arithmetic_type) values are the same within the
/// defined tolerance. Vectors must have the same size.
///
///@param result Container that stores calculated values
///@param correct Container that stores accurate values
///@param absolute_error Absolute error between any two pairs of resulting and correct value
///@return true if the maximum absolute error is lesser than ABSOLUTE_ERROR_EPSILON false otherwise
///
template <typename T>
bool TestEqualVectorPairs(const std::vector<std::pair<std::uint64_t, T>> &result,
                          const std::vector<std::pair<std::uint64_t, T>> &correct,
                          double absolute_error = ABSOLUTE_ERROR_EPSILON) {
  if (result.size() != correct.size()) return false;

  std::vector<std::pair<std::uint64_t, T>> result_cp(result);
  std::vector<std::pair<std::uint64_t, T>> correct_cp(correct);

  std::sort(result_cp.begin(), result_cp.end());
  std::sort(correct_cp.begin(), correct_cp.end());

  return std::equal(
      result_cp.begin(), result_cp.end(), correct_cp.begin(), [&absolute_error](auto result_cp, auto correct_cp) {
        return result_cp.first == correct_cp.first && std::abs(result_cp.second - correct_cp.second) <= absolute_error;
      });
}
}  // namespace mg_test_utility
