#pragma once

#include "communication/bolt/v1/encoder/base_encoder.hpp"

namespace durability {

template <typename Buffer>
class SnapshotEncoder : public communication::bolt::BaseEncoder<Buffer> {
 public:
  explicit SnapshotEncoder(Buffer &buffer)
      : communication::bolt::BaseEncoder<Buffer>(buffer) {}
  void WriteSnapshotVertex(const VertexAccessor &vertex) {
    communication::bolt::BaseEncoder<Buffer>::WriteVertex(vertex);

    // write in edges without properties
    this->WriteUInt(vertex.in_degree());
    auto edges_in = vertex.in();
    for (const auto &edge : edges_in) {
      this->WriteSnapshotEdge(edge, true);
    }

    // write out edges without properties
    this->WriteUInt(vertex.out_degree());
    auto edges_out = vertex.out();
    for (const auto &edge : edges_out) {
      this->WriteSnapshotEdge(edge, false);
    }
  }

 private:
  void WriteUInt(const uint64_t &value) {
    this->WriteInt(*reinterpret_cast<const int64_t *>(&value));
  }

  // Writes edge without properties
  void WriteSnapshotEdge(const EdgeAccessor &edge, bool write_from) {
    WriteUInt(edge.GlobalAddress().raw());
    if (write_from)
      WriteUInt(edge.from().GlobalAddress().raw());
    else
      WriteUInt(edge.to().GlobalAddress().raw());

    // write type
    this->WriteString(edge.db_accessor().EdgeTypeName(edge.EdgeType()));
  }
};

}  // namespace durability
