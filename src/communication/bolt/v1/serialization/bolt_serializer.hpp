#pragma once

#include "communication/bolt/v1/packing/codes.hpp"
#include "communication/bolt/v1/transport/bolt_encoder.hpp"
#include "communication/bolt/v1/transport/chunked_buffer.hpp"
#include "communication/bolt/v1/transport/chunked_encoder.hpp"

#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "storage/property_value_store.hpp"

namespace bolt {

template <class Stream>
class BoltSerializer {
 public:
  BoltSerializer(Stream &stream) : encoder(stream) {}

  /** Serializes the vertex accessor into the packstream format
   *
   * struct[size = 3] Vertex [signature = 0x4E] {
   *     Integer            node_id;
   *     List<String>       labels;
   *     Map<String, Value> properties;
   * }
   *
   */
  void write(const VertexAccessor &vertex) {
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
    const PropertyValueStore<GraphDb::Property> &props = vertex.Properties();
    encoder.write_map_header(props.size());
    props.Accept([this, &vertex](const GraphDb::Property prop,
                                 const PropertyValue &value) {
      this->encoder.write(vertex.db_accessor().property_name(prop));
      this->write(value);
    });
  }

  /** Serializes the edge accessor into the packstream format
   *
   * struct[size = 5] Edge [signature = 0x52] {
   *     Integer            edge_id;          // IMPORTANT: always 0 since we
   * don't do IDs
   *     Integer            start_node_id;    // IMPORTANT: always 0 since we
   * don't do IDs
   *     Integer            end_node_id;      // IMPORTANT: always 0 since we
   * don't do IDs
   *     String             type;
   *     Map<String, Value> properties;
   * }
   *
   */
  void write(const EdgeAccessor &edge) {
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
    const PropertyValueStore<GraphDb::Property> &props = edge.Properties();
    encoder.write_map_header(props.size());
    props.Accept(
        [this, &edge](GraphDb::Property prop, const PropertyValue &value) {
          this->encoder.write(edge.db_accessor().property_name(prop));
          this->write(value);
        });
  }

  // TODO document
  void write_failure(const std::map<std::string, std::string> &data) {
    encoder.message_failure();
    encoder.write_map_header(data.size());
    for (auto const &kv : data) {
      write(kv.first);
      write(kv.second);
    }
  }

  /**
   * Writes a PropertyValue (typically a property value in the edge or vertex).
   *
   * @param value The value to write.
   */
  void write(const PropertyValue &value) {
    switch (value.type()) {
      case PropertyValue::Type::Null:
        encoder.write_null();
        return;
      case PropertyValue::Type::Bool:
        encoder.write_bool(value.Value<bool>());
        return;
      case PropertyValue::Type::String:
        encoder.write_string(value.Value<std::string>());
        return;
      case PropertyValue::Type::Int:
        encoder.write_integer(value.Value<int64_t>());
        return;
      case PropertyValue::Type::Double:
        encoder.write_double(value.Value<double>());
        return;
      case PropertyValue::Type::List:
        // Not implemented
        assert(false);
    }
  }

 protected:
  Stream &encoder;
};
}
