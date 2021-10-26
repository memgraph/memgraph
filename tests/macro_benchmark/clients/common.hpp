// Copyright 2021 Memgraph Ltd.
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

#include <chrono>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <string>

#include "communication/bolt/client.hpp"
#include "communication/bolt/v1/value.hpp"
#include "utils/algorithm.hpp"
#include "utils/exceptions.hpp"
#include "utils/spin_lock.hpp"
#include "utils/timer.hpp"

using communication::ClientContext;
using communication::bolt::Client;
using communication::bolt::Value;
using io::network::Endpoint;

void PrintJsonValue(std::ostream &os, const Value &value) {
  switch (value.type()) {
    case Value::Type::Null:
      os << "null";
      break;
    case Value::Type::Bool:
      os << (value.ValueBool() ? "true" : "false");
      break;
    case Value::Type::Int:
      os << value.ValueInt();
      break;
    case Value::Type::Double:
      os << value.ValueDouble();
      break;
    case Value::Type::String:
      os << "\"" << value.ValueString() << "\"";
      break;
    case Value::Type::List:
      os << "[";
      utils::PrintIterable(os, value.ValueList(), ", ",
                           [](auto &stream, const auto &item) { PrintJsonValue(stream, item); });
      os << "]";
      break;
    case Value::Type::Map:
      os << "{";
      utils::PrintIterable(os, value.ValueMap(), ", ", [](auto &stream, const auto &pair) {
        PrintJsonValue(stream, {pair.first});
        stream << ": ";
        PrintJsonValue(stream, pair.second);
      });
      os << "}";
      break;
    default:
      std::terminate();
  }
}

std::pair<communication::bolt::QueryData, int> ExecuteNTimesTillSuccess(
    Client &client, const std::string &query, const std::map<std::string, communication::bolt::Value> &params,
    int max_attempts) {
  static thread_local std::mt19937 pseudo_rand_gen_{std::random_device{}()};
  static thread_local std::uniform_int_distribution<> rand_dist_{10, 50};
  int failed_attempts{0};
  while (true) {
    try {
      auto ret = client.Execute(query, params);
      return {ret, failed_attempts};
    } catch (const utils::BasicException &e) {
      spdlog::debug("Error: {}", e.what());
      if (++failed_attempts == max_attempts) {
        spdlog::warn("{} failed {}", query, failed_attempts);
        throw;
      }
      utils::Timer t;
      std::chrono::microseconds to_sleep(rand_dist_(pseudo_rand_gen_));
      while (t.Elapsed() < to_sleep)
        ;
    }
  }
}
