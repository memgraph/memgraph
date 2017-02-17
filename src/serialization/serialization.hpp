#pragma once

#include "database/db_accessor.hpp"
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
    // Serialize vertex id
    writer.start_vertex(v.id());

    // Serialize labels
    auto const &labels = v.labels();
    writer.label_count(labels.size());
    for (auto &label : labels) {
        writer.label(label.get().str());
    }

    // Serialize propertys
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
    // Serialize to and from vertices ids.
    writer.start_edge(e.from().id(), e.to().id());

    // Serialize type
    writer.edge_type(e.edge_type().str());

    // Serialize propertys
    auto const &propertys = e.properties();
    writer.property_count(propertys.size());
    for (auto &prop : propertys) {
        writer.property_name(prop.key.family_name());
        prop.accept_primitive(writer);
    }

    writer.end_edge();
}

// Deserializes vertex from reader into database db. Returns Id which vertex had
// in the reader and VertexAccessor.
template <class D>
std::pair<Id, VertexAccessor> deserialize_vertex(DbAccessor &db, D &reader)
{
    auto v = db.vertex_insert();
    auto old_id = reader.vertex_start();

    // Deserialize labels
    std::string s;
    for (auto i = reader.label_count(); i > 0; i--) {
        auto &label_key = db.label_find_or_create(reader.label().c_str());
        v.add_label(label_key);
    }

    // Deserialize propertys
    for (auto i = reader.property_count(); i > 0; i--) {
        auto &family =
            db.vertex_property_family_get(reader.property_name().c_str());
        v.set(StoredProperty<TypeGroupVertex>(
            family, reader.template property<Property>()));
    }

    reader.vertex_end();
    return std::make_pair(old_id, v);
}

// Deserializes edge from reader into database db. Returns loaded EdgeAccessor.
// S - is the storage with at() method for accesing VertexAccessor under thers
// deserialization local id returnd from deserialize_vertex.
template <class D, class S>
EdgeAccessor deserialize_edge(DbAccessor &db, D &reader, S &store)
{
    auto ids = reader.edge_start();
    // Deserialize from and to ids of vertices.
    VertexAccessor &from = store.at(ids.first);
    VertexAccessor &to = store.at(ids.second);

    auto e = db.edge_insert(from, to);

    // Deserialize type
    auto &edge_type_key = db.type_find_or_create(reader.edge_type().c_str());
    e.edge_type(edge_type_key);

    // Deserialize properties
    for (auto i = reader.property_count(); i > 0; i--) {
        auto &family =
            db.edge_property_family_get(reader.property_name().c_str());
        e.set(StoredProperty<TypeGroupEdge>(
            family, reader.template property<Property>()));
    }

    reader.edge_end();
    return e;
}
};
