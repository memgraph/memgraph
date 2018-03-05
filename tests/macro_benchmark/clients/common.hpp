#pragma once

#include <chrono>
#include <experimental/optional>
#include <map>
#include <random>
#include <string>

#include "communication/bolt/client.hpp"
#include "communication/bolt/v1/decoder/decoded_value.hpp"
#include "utils/exceptions.hpp"
#include "utils/timer.hpp"

namespace {

void PrintJsonDecodedValue(std::ostream &os,
                           const communication::bolt::DecodedValue &value) {
  using communication::bolt::DecodedValue;
  switch (value.type()) {
    case DecodedValue::Type::Null:
      os << "null";
      break;
    case DecodedValue::Type::Bool:
      os << (value.ValueBool() ? "true" : "false");
      break;
    case DecodedValue::Type::Int:
      os << value.ValueInt();
      break;
    case DecodedValue::Type::Double:
      os << value.ValueDouble();
      break;
    case DecodedValue::Type::String:
      os << "\"" << value.ValueString() << "\"";
      break;
    case DecodedValue::Type::List:
      os << "[";
      utils::PrintIterable(os, value.ValueList(), ", ",
                           [](auto &stream, const auto &item) {
                             PrintJsonDecodedValue(stream, item);
                           });
      os << "]";
      break;
    case DecodedValue::Type::Map:
      os << "{";
      utils::PrintIterable(os, value.ValueMap(), ", ",
                           [](auto &stream, const auto &pair) {
                             PrintJsonDecodedValue(stream, {pair.first});
                             stream << ": ";
                             PrintJsonDecodedValue(stream, pair.second);
                           });
      os << "}";
      break;
    default:
      std::terminate();
  }
}

template <typename TClient>
std::pair<communication::bolt::QueryData, int> ExecuteNTimesTillSuccess(
    TClient &client, const std::string &query,
    const std::map<std::string, communication::bolt::DecodedValue> &params,
    int times) {
  std::experimental::optional<utils::BasicException> last_exception;
  static thread_local std::mt19937 pseudo_rand_gen_{std::random_device{}()};
  static thread_local std::uniform_int_distribution<> rand_dist_{10, 50};
  for (int i = 0; i < times; ++i) {
    try {
      auto ret = client.Execute(query, params);
      return {ret, i};
    } catch (const utils::BasicException &e) {
      last_exception = e;
      utils::Timer t;
      std::chrono::microseconds to_sleep(rand_dist_(pseudo_rand_gen_));
      while (t.Elapsed() < to_sleep) {
        cpu_relax();
      }
    }
  }
  LOG(WARNING) << query << " failed " << times << "times";
  throw last_exception.value();
}
}  // namespace
