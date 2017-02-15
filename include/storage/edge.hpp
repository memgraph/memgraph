#pragma once

#include "database/graph_db.hpp"
#include "mvcc/record.hpp"
#include "mvcc/version_list.hpp"
#include "storage/typed_value_store.hpp"

// forward declare Vertex because there is a circular usage Edge <-> Vertex
class Vertex;

class Edge : public mvcc::Record<Edge> {
public:

  Edge(mvcc::VersionList<Vertex>& from,
       mvcc::VersionList<Vertex>& to,
       GraphDb::EdgeType edge_type)
      : from_(from), to_(to), edge_type_(edge_type) {}

  mvcc::VersionList<Vertex>& from_;
  mvcc::VersionList<Vertex>& to_;
  GraphDb::EdgeType edge_type_;
  TypedValueStore<GraphDb::Property> properties_;
};
