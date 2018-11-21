#pragma once

#include <map>
#include <string>
#include <vector>

#include "communication/bolt/v1/value.hpp"
#include "storage/common/types/property_value.hpp"
#include "storage/distributed/address_types.hpp"
#include "utils/algorithm.hpp"
#include "utils/exceptions.hpp"

namespace durability {

/** Forward declartion of SnapshotEdge. */
struct InlinedVertexEdge;

/**
 * Structure used when reading a Vertex with the decoder.
 * The decoder writes data into this structure.
 */
struct SnapshotVertex {
  gid::Gid gid;
  int64_t cypher_id;
  std::vector<std::string> labels;
  std::map<std::string, communication::bolt::Value> properties;
  // Vector of edges without properties
  std::vector<InlinedVertexEdge> in;
  std::vector<InlinedVertexEdge> out;
};

/**
 * Structure used when reading an Edge with the snapshot decoder.
 * The decoder writes data into this structure.
 */
struct InlinedVertexEdge {
  // Addresses down below must always be global_address and never direct
  // pointers to a record.
  storage::EdgeAddress address;
  storage::VertexAddress vertex;
  std::string type;
};

}  // namespace durability
