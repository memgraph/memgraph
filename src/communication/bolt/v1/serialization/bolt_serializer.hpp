#pragma once

#include "communication/bolt/v1/packing/codes.hpp"
#include "communication/bolt/v1/transport/bolt_encoder.hpp"

#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"

#include "storage/property_value.hpp"

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
  void write(const VertexAccessor &vertex);

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
  void write(const EdgeAccessor &edge);

  // TODO document
  void write_failure(const std::map<std::string, std::string> &data);

  /**
   * Writes a PropertyValue (typically a property value in the edge or vertex).
   *
   * @param value The value to write.
   */
  void write(const PropertyValue &value);

 protected:
  Stream &encoder;
};
}
