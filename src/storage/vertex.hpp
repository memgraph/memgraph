#pragma once

#include <vector>

#include "database/graph_db.hpp"
#include "mvcc/record.hpp"
#include "mvcc/version_list.hpp"
#include "storage/property_value_store.hpp"

// forward declare Edge because there is a circular usage Edge <-> Vertex
class Edge;

class Vertex : public mvcc::Record<Vertex> {
 public:
  std::vector<mvcc::VersionList<Edge>*> out_;
  std::vector<mvcc::VersionList<Edge>*> in_;
  std::vector<GraphDb::Label> labels_;
  PropertyValueStore<GraphDb::Property> properties_;
};
