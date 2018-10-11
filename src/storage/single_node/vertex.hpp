#pragma once

#include "mvcc/single_node/record.hpp"
#include "mvcc/single_node/version_list.hpp"
#include "storage/common/property_value_store.hpp"
#include "storage/common/types.hpp"
#include "storage/single_node/edges.hpp"

class Vertex : public mvcc::Record<Vertex> {
 public:
  Vertex() = default;
  // Returns new Vertex with copy of data stored in this Vertex, but without
  // copying superclass' members.
  Vertex *CloneData() { return new Vertex(*this); }

  Edges out_;
  Edges in_;
  std::vector<storage::Label> labels_;
  PropertyValueStore properties_;

 private:
  Vertex(const Vertex &other)
      : mvcc::Record<Vertex>(),
        out_(other.out_),
        in_(other.in_),
        labels_(other.labels_),
        properties_(other.properties_) {}
};
