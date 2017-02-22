#include "communication/bolt/v1/serialization/bolt_serializer.hpp"

#include "communication/bolt/v1/transport/chunked_buffer.hpp"
#include "communication/bolt/v1/transport/chunked_encoder.hpp"
#include "communication/bolt/v1/transport/socket_stream.hpp"
#include "io/network/socket.hpp"

#include "database/graph_db.hpp"
#include "storage/typed_value_store.hpp"

template <class Stream>
void bolt::BoltSerializer<Stream>::write(const VertexAccessor &vertex) {
  // write signatures for the node struct and node data type
  encoder.write_struct_header(3);
  encoder.write(underlying_cast(pack::Code::Node));

  // IMPORTANT: here we write a hardcoded 0 because we don't
  // use internal IDs, but need to give something to Bolt
  // note that OpenCypher has no id(x) function, so the client
  // should not be able to do anything with this value anyway
  encoder.write_integer(0);  // uID

  // write the list of labels
  auto labels = vertex.labels();
  encoder.write_list_header(labels.size());
  for (auto label : labels)
    encoder.write_string(vertex.db_accessor().label_name(label));

  // write the properties
  const TypedValueStore<GraphDb::Property> &props = vertex.Properties();
  encoder.write_map_header(props.size());
  props.Accept(
      [this, &vertex](const GraphDb::Property prop, const TypedValue &value) {
        this->encoder.write(vertex.db_accessor().property_name(prop));
        this->write(value);
      });
}

template <class Stream>
void bolt::BoltSerializer<Stream>::write(const EdgeAccessor &edge) {
  // write signatures for the edge struct and edge data type
  encoder.write_struct_header(5);
  encoder.write(underlying_cast(pack::Code::Relationship));

  // IMPORTANT: here we write a hardcoded 0 because we don't
  // use internal IDs, but need to give something to Bolt
  // note that OpenCypher has no id(x) function, so the client
  // should not be able to do anything with this value anyway
  encoder.write_integer(0);
  encoder.write_integer(0);
  encoder.write_integer(0);

  // write the type of the edge
  encoder.write(edge.db_accessor().edge_type_name(edge.edge_type()));

  // write the property map
  const TypedValueStore<GraphDb::Property> &props = edge.Properties();
  encoder.write_map_header(props.size());
  props.Accept([this, &edge](GraphDb::Property prop, const TypedValue &value) {
    this->encoder.write(edge.db_accessor().property_name(prop));
    this->write(value);
  });
}

template <class Stream>
void bolt::BoltSerializer<Stream>::write(const TypedValue &value) {
  switch (value.type_) {
    case TypedValue::Type::Null:
      encoder.write_null();
      return;
    case TypedValue::Type::Bool:
      encoder.write_bool(value.Value<bool>());
      return;
    case TypedValue::Type::String:
      encoder.write_string(value.Value<std::string>());
      return;
    case TypedValue::Type::Int:
      encoder.write_integer(value.Value<int>());
      return;
    case TypedValue::Type::Float:
      encoder.write_double(value.Value<float>());
      return;
  }
}

template <class Stream>
void bolt::BoltSerializer<Stream>::write_failure(
    const std::map<std::string, std::string> &data) {
  encoder.message_failure();
  encoder.write_map_header(data.size());
  for (auto const &kv : data) {
    write(kv.first);
    write(kv.second);
  }
}

template class bolt::BoltSerializer<bolt::BoltEncoder<
    bolt::ChunkedEncoder<bolt::ChunkedBuffer<bolt::SocketStream<io::Socket>>>>>;
