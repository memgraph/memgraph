#pragma once

#include "communication/bolt/v1/encoder/base_encoder.hpp"
#include "utils/cast.hpp"

namespace durability {

template <typename Buffer>
class SnapshotEncoder : public communication::bolt::BaseEncoder<Buffer> {
 public:
  explicit SnapshotEncoder(Buffer &buffer)
      : communication::bolt::BaseEncoder<Buffer>(buffer) {}
  void WriteSnapshotVertex(const VertexAccessor &vertex) {
    communication::bolt::BaseEncoder<Buffer>::WriteVertex(vertex);

    // Write cypher_id
    this->WriteInt(vertex.cypher_id());

    // Write in edges without properties
    this->WriteUInt(vertex.in_degree());
    auto edges_in = vertex.in();
    for (const auto &edge : edges_in) {
      this->WriteSnapshotEdge(edge, true);
    }

    // Write out edges without properties
    this->WriteUInt(vertex.out_degree());
    auto edges_out = vertex.out();
    for (const auto &edge : edges_out) {
      this->WriteSnapshotEdge(edge, false);
    }
  }

 private:
  void WriteUInt(const uint64_t &value) {
    this->WriteInt(utils::MemcpyCast<int64_t>(value));
  }

  // Writes edge without properties
  void WriteSnapshotEdge(const EdgeAccessor &edge, bool write_from) {
    // Write global id of the edge
    WriteUInt(edge.GlobalAddress().raw());

    // Write to/from global id
    if (write_from)
      WriteUInt(edge.from().GlobalAddress().raw());
    else
      WriteUInt(edge.to().GlobalAddress().raw());

    // Write type
    this->WriteString(edge.db_accessor().EdgeTypeName(edge.EdgeType()));
  }
};

}  // namespace durability
