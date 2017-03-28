#pragma once

#include "communication/bolt/v1/encoder/chunked_buffer.hpp"
#include "communication/bolt/v1/encoder/encoder.hpp"
#include "query/backend/cpp/typed_value.hpp"

#include "logging/default.hpp"

namespace communication::bolt {

/**
 * A high level API for streaming a Bolt response. Exposes
 * functionalities used by the compiler and query plans (which
 * should not use any lower level API).
 *
 * @tparam Encoder Encoder used.
 */
template <typename Encoder>
class ResultStream {
 public:
  ResultStream(Encoder &encoder) : encoder_(encoder) {}

  /**
   * Writes a header. Typically a header is something like:
   * [ "Header1", "Header2", "Header3" ]
   *
   * @param fields the header fields that should be sent.
   */
  void Header(const std::vector<std::string> &fields) {
    std::vector<TypedValue> vec;
    std::map<std::string, TypedValue> data;
    for (auto &i : fields) vec.push_back(TypedValue(i));
    data.insert(std::make_pair(std::string("fields"), TypedValue(vec)));
    // this call will automaticaly send the data to the client
    encoder_.MessageSuccess(data);
  }

  /**
   * Writes a result. Typically a result is something like:
   * [
   *     Value1,
   *     Value2,
   *     Value3
   * ]
   * NOTE: The result fields should be in the same ordering that the header
   *       fields were sent in.
   *
   * @param values the values that should be sent
   */
  void Result(std::vector<TypedValue> &values) {
    encoder_.MessageRecord(values);
  }

  /**
   * Writes a summary. Typically a summary is something like:
   * {
   *    "type" : "r" | "rw" | ...,
   *    "stats": {
   *        "nodes_created": 12,
   *        "nodes_deleted": 0
   *     }
   * }
   *
   * @param summary the summary map object that should be sent
   */
  void Summary(const std::map<std::string, TypedValue> &summary) {
    // at this point message should not flush the socket so
    // here is false because chunk has to be called instead of flush
    encoder_.MessageSuccess(summary, false);
  }

 private:
  Encoder &encoder_;
};
}
