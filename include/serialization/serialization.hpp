#pragma once

#include "storage/edge_accessor.hpp"
#include "storage/edge_type/edge_type.hpp"
#include "storage/label/label.hpp"
#include "storage/vertex_accessor.hpp"

namespace serialization
{

// Serializes Vertex to given writer which implements GraphEncoder.
template <class W>
void serialize_vertex(VertexAccessor const &v, W &writer)
{
    writer.start_vertex(v.id());

    auto const &labels = v.labels();
    writer.label_count(labels.size());
    for (auto &label : labels) {
        writer.label(label.get().str());
    }

    auto const &propertys = v.properties();
    writer.property_count(propertys.size());
    for (auto &prop : propertys) {
        writer.property_name(prop.key.family_name());
        prop.accept_primitive(writer);
    }

    writer.end_vertex();
}

// Serializes Edge to given writer which implements GraphEncoder.
template <class W>
void serialize_edge(EdgeAccessor const &e, W &writer)
{
    writer.start_edge(e.id(), e.from().id(), e.to().id());

    writer.edge_type(e.edge_type().str());

    auto const &propertys = e.properties();
    writer.property_count(propertys.size());
    for (auto &prop : propertys) {
        writer.property_name(prop.key.family_name());
        prop.accept_primitive(writer);
    }

    writer.end_edge();
}
};
