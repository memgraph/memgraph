#pragma once

#include <map>
#include <string>
#include <vector>

#include "communication/bolt/v1/decoder/decoded_value.hpp"
#include "query/typed_value.hpp"
#include "storage/address_types.hpp"
#include "storage/property_value.hpp"
#include "utils/algorithm.hpp"
#include "utils/exceptions.hpp"

namespace durability {

/** Forward declartion of DecodedSnapshotEdge. */
struct DecodedInlinedVertexEdge;

/**
 * Structure used when reading a Vertex with the decoder.
 * The decoder writes data into this structure.
 */
struct DecodedSnapshotVertex {
  gid::Gid gid;
  int64_t cypher_id;
  std::vector<std::string> labels;
  std::map<std::string, communication::bolt::DecodedValue> properties;
  // Vector of edges without properties
  std::vector<DecodedInlinedVertexEdge> in;
  std::vector<DecodedInlinedVertexEdge> out;
};

/**
 * Structure used when reading an Edge with the snapshot decoder.
 * The decoder writes data into this structure.
 */
struct DecodedInlinedVertexEdge {
  // Addresses down below must always be global_address and never direct
  // pointers to a record.
  storage::EdgeAddress address;
  storage::VertexAddress vertex;
  std::string type;
};

}  // namespace durability
