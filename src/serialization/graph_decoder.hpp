#pragma once

#include "utils/array_store.hpp"
#include "utils/void.hpp"

// Common menthods for translating Vertex/Edge representations from serialized
// form into database form.
// Implementor should override those methods which he needs, and ignore the
// rest.
// Caller is responisble to structure his calls as following:
//
//
// End goal would be to enforce these rules during compile time.
class GraphDecoder {
 public:
  // Starts reading vertex.
  Id vertex_start();

  // Returns number of stored labels.
  size_t label_count();

  // Wiil read label into given storage.
  std::string const &label();

  // Ends reading vertex
  void vertex_end() {}

  // Starts reading edge. Return from to ids of connected vertices.
  std::pair<Id, Id> edge_start();

  // Reads edge_type into given storage.
  std::string const &edge_type();

  // Ends reading edge.
  void edge_end() {}

  // Returns number of stored propertys.
  size_t property_count();

  // Reads property name into given storage.
  std::string const &property_name();

  // Reads property and calls T::handle for that property .
  template <class T>
  T property();
};
