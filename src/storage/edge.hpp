#pragma once

#include "database/graph_db_datatypes.hpp"
#include "mvcc/record.hpp"
#include "mvcc/version_list.hpp"
#include "storage/property_value_store.hpp"

// forward declare Vertex because there is a circular usage Edge <-> Vertex
class Vertex;

class Edge : public mvcc::Record<Edge> {
 public:
  Edge(mvcc::VersionList<Vertex> &from, mvcc::VersionList<Vertex> &to,
       GraphDbTypes::EdgeType edge_type)
      : from_(from), to_(to), edge_type_(edge_type) {}

  mvcc::VersionList<Vertex> &from_;
  mvcc::VersionList<Vertex> &to_;
  GraphDbTypes::EdgeType edge_type_;
  PropertyValueStore<GraphDbTypes::Property> properties_;
};
