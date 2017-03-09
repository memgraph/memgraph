#pragma once

#include "communication/bolt/v1/serialization/bolt_serializer.hpp"
#include "query/backend/cpp/typed_value.hpp"

#include "logging/default.hpp"

namespace bolt {

/**
 * A high level API for streaming a Bolt response. Exposes
 * functionalities used by the compiler and query plans (which
 * should not use any lower level API).
 *
 * @tparam TChunkedEncoder Type of chunked encoder used.
 */
// TODO templatisation on TChunkedEncoder might not be desired
// but makes the code a bit easer to understand because we know
// that this class uses a BoltEncoder (and not some arbitrary template)
// it helps the programmer, the compiler and the IDE
template <typename TChunkedEncoder>
class RecordStream {
 public:
  // TODO add logging to this class

  RecordStream(BoltEncoder<TChunkedEncoder> &bolt_encoder)
      : bolt_encoder_(bolt_encoder), serializer_(bolt_encoder) {}

  void Header(const std::vector<std::string> &fields) {
    bolt_encoder_.message_success();
    bolt_encoder_.write_map_header(1);
    bolt_encoder_.write_string("fields");

    bolt_encoder_.write_list_header(fields.size());
    for (auto &name : fields) {
      bolt_encoder_.write_string(name);
    }

    Chunk();
    Send();
  }

  void Result(std::vector<TypedValue> &values) {
    bolt_encoder_.message_record();
    Write(values);
    Chunk();
    Send();
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
   * @param value
   */
  void Summary(const std::map<std::string, TypedValue> &summary) {
    bolt_encoder_.message_success();
    Write(summary);
    Chunk();
  }

 private:
  BoltEncoder<TChunkedEncoder> bolt_encoder_;
  BoltSerializer<BoltEncoder<TChunkedEncoder>> serializer_;

  /**
   * Writes a TypedValue. Resolves it's type and uses
   * encoder primitives to write exactly typed values.
   */
  void Write(const TypedValue &value) {
    switch (value.type()) {
      case TypedValue::Type::Null:
        bolt_encoder_.write_null();
        break;
      case TypedValue::Type::Bool:
        bolt_encoder_.write(value.Value<bool>());
        break;
      case TypedValue::Type::Int:
        bolt_encoder_.write(value.Value<int64_t>());
        break;
      case TypedValue::Type::Double:
        bolt_encoder_.write(value.Value<double>());
        break;
      case TypedValue::Type::String:
        bolt_encoder_.write(value.Value<std::string>());
        break;
      case TypedValue::Type::List:
        Write(value.Value<std::vector<TypedValue>>());
        break;
      case TypedValue::Type::Map:
        Write(value.Value<std::map<std::string, TypedValue>>());
        break;
      case TypedValue::Type::Vertex:
        serializer_.write(value.Value<VertexAccessor>());
        break;
      case TypedValue::Type::Edge:
        serializer_.write(value.Value<EdgeAccessor>());
        break;
      default:
        throw std::runtime_error(
            "Serialization not implemented for given type");
    }
  }

  void Write(const std::vector<TypedValue> &values) {
    bolt_encoder_.write_list_header(values.size());
    for (const auto &value : values) Write(value);
  }

  void Write(const std::map<std::string, TypedValue> &values) {
    bolt_encoder_.write_map_header(values.size());
    for (const auto &kv : values) {
      bolt_encoder_.write(kv.first);
      Write(kv.second);
    }
  }

  void Send() {
    // TODO expose these low level functions in the encoder
    // be careful! ChunkedEncoder seems to have a 'flush()' function
    // but that is different from it's underlying ChunkedBuffer's
    // 'flush()' method
    bolt_encoder_.flush();
  }

  void Chunk() {
    // TODO expose these low level functions in the encoder
    bolt_encoder_.write_chunk();
  }
};
}
