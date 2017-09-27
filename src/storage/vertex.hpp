#pragma once

#include "database/graph_db_datatypes.hpp"
#include "mvcc/record.hpp"
#include "storage/edges.hpp"
#include "storage/property_value_store.hpp"

class Vertex : public mvcc::Record<Vertex> {
 public:
  Vertex() = default;
  // Returns new Vertex with copy of data stored in this Vertex, but without
  // copying superclass' members.
  Vertex *CloneData() { return new Vertex(*this); }

  Edges out_;
  Edges in_;
  std::vector<GraphDbTypes::Label> labels_;
  PropertyValueStore<GraphDbTypes::Property> properties_;

 private:
  Vertex(const Vertex &other)
      : mvcc::Record<Vertex>(),
        out_(other.out_),
        in_(other.in_),
        labels_(other.labels_),
        properties_(other.properties_) {}
};
