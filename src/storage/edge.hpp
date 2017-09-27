#pragma once

#include "database/graph_db_datatypes.hpp"
#include "mvcc/record.hpp"
#include "mvcc/version_list.hpp"
#include "storage/property_value_store.hpp"

class Vertex;

class Edge : public mvcc::Record<Edge> {
 public:
  Edge(mvcc::VersionList<Vertex> &from, mvcc::VersionList<Vertex> &to,
       GraphDbTypes::EdgeType edge_type)
      : from_(from), to_(to), edge_type_(edge_type) {}
  // Returns new Edge with copy of data stored in this Edge, but without
  // copying superclass' members.
  Edge *CloneData() { return new Edge(*this); }

  mvcc::VersionList<Vertex> &from_;
  mvcc::VersionList<Vertex> &to_;
  GraphDbTypes::EdgeType edge_type_;
  PropertyValueStore<GraphDbTypes::Property> properties_;

 private:
  Edge(const Edge &other)
      : mvcc::Record<Edge>(),
        from_(other.from_),
        to_(other.to_),
        edge_type_(other.edge_type_),
        properties_(other.properties_) {}
};
;
