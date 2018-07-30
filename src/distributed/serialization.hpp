#pragma once

#include <cstdint>
#include <memory>

#include "distributed/serialization.capnp.h"
#include "storage/edge.hpp"
#include "storage/vertex.hpp"

namespace distributed {

void SaveVertex(const Vertex &vertex, capnp::Vertex::Builder *builder,
                int16_t worker_id);

void SaveEdge(const Edge &edge, capnp::Edge::Builder *builder,
              int16_t worker_id);

/// Alias for `SaveEdge` allowing for param type resolution.
inline void SaveElement(const Edge &record, capnp::Edge::Builder *builder,
                        int16_t worker_id) {
  return SaveEdge(record, builder, worker_id);
}

/// Alias for `SaveVertex` allowing for param type resolution.
inline void SaveElement(const Vertex &record, capnp::Vertex::Builder *builder,
                        int16_t worker_id) {
  return SaveVertex(record, builder, worker_id);
}

std::unique_ptr<Vertex> LoadVertex(const capnp::Vertex::Reader &reader);

std::unique_ptr<Edge> LoadEdge(const capnp::Edge::Reader &reader);

}  // namespace distributed
