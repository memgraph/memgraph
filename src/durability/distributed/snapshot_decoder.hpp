#pragma once

#include <experimental/optional>

#include "communication/bolt/v1/decoder/decoder.hpp"
#include "durability/distributed/snapshot_value.hpp"

namespace durability {

template <typename Buffer>
class SnapshotDecoder : public communication::bolt::Decoder<Buffer> {
 public:
  explicit SnapshotDecoder(Buffer &buffer)
      : communication::bolt::Decoder<Buffer>(buffer) {}

  std::experimental::optional<SnapshotVertex> ReadSnapshotVertex() {
    communication::bolt::Value dv;
    SnapshotVertex vertex;

    // Read global id, labels and properties of the vertex
    if (!communication::bolt::Decoder<Buffer>::ReadValue(
            &dv, communication::bolt::Value::Type::Vertex)) {
      DLOG(WARNING) << "Unable to read snapshot vertex";
      return std::experimental::nullopt;
    }
    auto &read_vertex = dv.ValueVertex();
    vertex.gid = read_vertex.id.AsUint();
    vertex.labels = read_vertex.labels;
    vertex.properties = read_vertex.properties;

    // Read cypher_id
    if (!communication::bolt::Decoder<Buffer>::ReadValue(
            &dv, communication::bolt::Value::Type::Int)) {
      DLOG(WARNING) << "Unable to read vertex cypher_id";
      return std::experimental::nullopt;
    }
    vertex.cypher_id = dv.ValueInt();

    // Read in edges
    if (!communication::bolt::Decoder<Buffer>::ReadValue(
            &dv, communication::bolt::Value::Type::Int)) {
      DLOG(WARNING) << "[ReadSnapshotVertex] Couldn't read number of in "
                       "edges in vertex!";
      return std::experimental::nullopt;
    }
    for (int i = 0; i < dv.ValueInt(); ++i) {
      auto edge = ReadSnapshotEdge();
      if (!edge) return std::experimental::nullopt;
      vertex.in.emplace_back(*edge);
    }

    // Read out edges
    if (!communication::bolt::Decoder<Buffer>::ReadValue(
            &dv, communication::bolt::Value::Type::Int)) {
      DLOG(WARNING) << "[ReadSnapshotVertex] Couldn't read number of out "
                       "edges in vertex!";
      return std::experimental::nullopt;
    }
    for (int i = 0; i < dv.ValueInt(); ++i) {
      auto edge = ReadSnapshotEdge();
      if (!edge) return std::experimental::nullopt;
      vertex.out.emplace_back(*edge);
    }

    VLOG(20) << "[ReadSnapshotVertex] Success";
    return vertex;
  }

 private:
  std::experimental::optional<InlinedVertexEdge> ReadSnapshotEdge() {
    communication::bolt::Value dv;
    InlinedVertexEdge edge;

    VLOG(20) << "[ReadSnapshotEdge] Start";

    // Read global id of this edge
    if (!communication::bolt::Decoder<Buffer>::ReadValue(
            &dv, communication::bolt::Value::Type::Int)) {
      DLOG(WARNING) << "[ReadSnapshotEdge] Couldn't read Global ID!";
      return std::experimental::nullopt;
    }
    edge.address = storage::EdgeAddress(static_cast<uint64_t>(dv.ValueInt()));

    // Read global vertex id of the other side of the edge
    // (global id of from/to vertexes).
    if (!communication::bolt::Decoder<Buffer>::ReadValue(
            &dv, communication::bolt::Value::Type::Int)) {
      DLOG(WARNING) << "[ReadSnapshotEdge] Couldn't read from/to address!";
      return std::experimental::nullopt;
    }
    edge.vertex = storage::VertexAddress(static_cast<uint64_t>(dv.ValueInt()));

    // Read edge type
    if (!communication::bolt::Decoder<Buffer>::ReadValue(
            &dv, communication::bolt::Value::Type::String)) {
      DLOG(WARNING) << "[ReadSnapshotEdge] Couldn't read type!";
      return std::experimental::nullopt;
    }
    edge.type = dv.ValueString();

    VLOG(20) << "[ReadSnapshotEdge] Success";
    return edge;
  }
};
};  // namespace durability
