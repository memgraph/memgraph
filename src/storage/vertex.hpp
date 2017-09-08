#pragma once

#include "database/graph_db_datatypes.hpp"
#include "mvcc/record.hpp"
#include "storage/edges.hpp"
#include "storage/property_value_store.hpp"

// forward declare Edge because there is a circular usage Edge <-> Vertex
class Edge;

class Vertex : public mvcc::Record<Vertex> {
 public:
  Edges out_;
  Edges in_;
  std::vector<GraphDbTypes::Label> labels_;
  PropertyValueStore<GraphDbTypes::Property> properties_;
};
