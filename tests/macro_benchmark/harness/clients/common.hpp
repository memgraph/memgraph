#include <experimental/optional>
#include <map>
#include <string>

#include "communication/bolt/client.hpp"
#include "communication/bolt/v1/decoder/decoded_value.hpp"
#include "utils/exceptions.hpp"

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
      PrintIterable(os, value.ValueList(), ", ",
                    [](auto &stream, const auto &item) {
                      PrintJsonDecodedValue(stream, item);
                    });
      os << "]";
      break;
    case DecodedValue::Type::Map:
      os << "{";
      PrintIterable(os, value.ValueMap(), ", ",
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
communication::bolt::QueryData ExecuteNTimesTillSuccess(
    TClient &client, const std::string &query,
    const std::map<std::string, communication::bolt::DecodedValue> &params,
    int times) {
  std::experimental::optional<utils::BasicException> last_exception;
  for (int i = 0; i < times; ++i) {
    try {
      auto ret = client.Execute(query, params);
      return ret;
    } catch (const utils::BasicException &e) {
      last_exception = e;
    }
  }
  throw last_exception;
}
}
