#pragma once

#include <experimental/optional>

#include "communication/bolt/v1/decoder/decoder.hpp"
#include "durability/snapshot_decoded_value.hpp"

namespace durability {

using namespace communication::bolt;

template <typename Buffer>
class SnapshotDecoder : public Decoder<Buffer> {
 public:
  explicit SnapshotDecoder(Buffer &buffer) : Decoder<Buffer>(buffer) {}

  std::experimental::optional<DecodedSnapshotVertex> ReadSnapshotVertex() {
    DecodedValue dv;
    DecodedSnapshotVertex vertex;

    if (!Decoder<Buffer>::ReadValue(&dv, DecodedValue::Type::Vertex)) {
      DLOG(WARNING) << "Unable to read snapshot vertex";
      return std::experimental::nullopt;
    }

    auto &read_vertex = dv.ValueVertex();
    vertex.gid = read_vertex.id;
    vertex.labels = read_vertex.labels;
    vertex.properties = read_vertex.properties;

    if (!Decoder<Buffer>::ReadValue(&dv, DecodedValue::Type::Int)) {
      DLOG(WARNING) << "[ReadSnapshotVertex] Couldn't read number of in "
                       "edges in vertex!";
      return std::experimental::nullopt;
    }

    for (int i = 0; i < dv.ValueInt(); ++i) {
      auto edge = ReadSnapshotEdge();
      if (!edge) return std::experimental::nullopt;
      vertex.in.emplace_back(*edge);
    }

    if (!Decoder<Buffer>::ReadValue(&dv, DecodedValue::Type::Int)) {
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
  std::experimental::optional<DecodedInlinedVertexEdge> ReadSnapshotEdge() {
    DecodedValue dv;
    DecodedInlinedVertexEdge edge;

    VLOG(20) << "[ReadSnapshotEdge] Start";

    // read ID
    if (!Decoder<Buffer>::ReadValue(&dv, DecodedValue::Type::Int)) {
      DLOG(WARNING) << "[ReadSnapshotEdge] Couldn't read ID!";
      return std::experimental::nullopt;
    }

    edge.address = dv.ValueInt();
    // read other side
    if (!Decoder<Buffer>::ReadValue(&dv, DecodedValue::Type::Int)) {
      DLOG(WARNING) << "[ReadSnapshotEdge] Couldn't read from address!";
      return std::experimental::nullopt;
    }
    edge.vertex = dv.ValueInt();

    // read type
    if (!Decoder<Buffer>::ReadValue(&dv, DecodedValue::Type::String)) {
      DLOG(WARNING) << "[ReadSnapshotEdge] Couldn't read type!";
      return std::experimental::nullopt;
    }
    edge.type = dv.ValueString();

    VLOG(20) << "[ReadSnapshotEdge] Success";

    return edge;
  }
};
};  // namespace durability
