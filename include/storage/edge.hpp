#pragma once

#include "database/graph_db.hpp"
#include "mvcc/record.hpp"
#include "mvcc/version_list.hpp"
#include "storage/typed_value_store.hpp"

// forward declare Vertex because there is a circular usage Edge <-> Vertex
class Vertex;

class Edge : public mvcc::Record<Edge> {
public:
  mvcc::VersionList<Vertex>* from_;
  mvcc::VersionList<Vertex>* to_;
  GraphDb::EdgeType edge_type_;
  TypedValueStore properties_;
};
