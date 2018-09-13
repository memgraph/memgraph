#pragma once

#include "storage/edge.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/property_value.hpp"
#include "storage/property_value_store.hpp"
#include "storage/serialization.capnp.h"
#include "storage/vertex.hpp"
#include "storage/vertex_accessor.hpp"

namespace database {
class GraphDbAccessor;
}

namespace distributed {
class DataManager;
}

namespace storage {

void SaveCapnpPropertyValue(const PropertyValue &value,
                            capnp::PropertyValue::Builder *builder);

void LoadCapnpPropertyValue(const capnp::PropertyValue::Reader &reader,
                            PropertyValue *value);

void SaveProperties(const PropertyValueStore &properties,
                    capnp::PropertyValueStore::Builder *builder);

void LoadProperties(const capnp::PropertyValueStore::Reader &reader,
                    PropertyValueStore *properties);

void SaveVertex(const Vertex &vertex, capnp::Vertex::Builder *builder,
                int16_t worker_id);

void SaveEdge(const Edge &edge, capnp::Edge::Builder *builder,
              int16_t worker_id);

/// Alias for `SaveEdge` allowing for param type resolution.
void SaveElement(const Edge &record, capnp::Edge::Builder *builder,
                 int16_t worker_id);

/// Alias for `SaveVertex` allowing for param type resolution.
void SaveElement(const Vertex &record, capnp::Vertex::Builder *builder,
                 int16_t worker_id);

std::unique_ptr<Vertex> LoadVertex(const capnp::Vertex::Reader &reader);

std::unique_ptr<Edge> LoadEdge(const capnp::Edge::Reader &reader);

enum class SendVersions { BOTH, ONLY_OLD, ONLY_NEW };

void SaveVertexAccessor(const VertexAccessor &vertex_accessor,
                        capnp::VertexAccessor::Builder *builder,
                        SendVersions versions, int worker_id);

VertexAccessor LoadVertexAccessor(const capnp::VertexAccessor::Reader &reader,
                                  database::GraphDbAccessor *dba,
                                  distributed::DataManager *data_manager);

void SaveEdgeAccessor(const EdgeAccessor &edge_accessor,
                      capnp::EdgeAccessor::Builder *builder,
                      SendVersions versions, int worker_id);

EdgeAccessor LoadEdgeAccessor(const capnp::EdgeAccessor::Reader &reader,
                              database::GraphDbAccessor *dba,
                              distributed::DataManager *data_manager);

}  // namespace storage
