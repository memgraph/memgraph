#pragma once

#include "communication/bolt/v1/encoder/encoder.hpp"
#include "communication/bolt/v1/encoder/chunked_buffer.hpp"
#include "query/backend/cpp/typed_value.hpp"

#include "logging/default.hpp"

namespace communication::bolt {

/**
 * A high level API for streaming a Bolt response. Exposes
 * functionalities used by the compiler and query plans (which
 * should not use any lower level API).
 *
 * @tparam Socket Socket used.
 */
template <typename Socket>
class ResultStream {
 private:
  using encoder_t = Encoder<ChunkedBuffer<Socket>, Socket>;
 public:

  // TODO add logging to this class
  ResultStream(encoder_t &encoder) :
      encoder_(encoder) {}

  /**
   * Writes a header. Typically a header is something like:
   * [
   *     "Header1",
   *     "Header2",
   *     "Header3"
   * ]
   *
   * @param fields the header fields that should be sent.
   */
  void Header(const std::vector<std::string> &fields) {
    std::vector<TypedValue> vec;
    std::map<std::string, TypedValue> data;
    for (auto& i : fields)
      vec.push_back(TypedValue(i));
    data.insert(std::make_pair(std::string("fields"), TypedValue(vec)));
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
    encoder_.MessageSuccess(summary);
  }

private:
  encoder_t& encoder_;
};
}
