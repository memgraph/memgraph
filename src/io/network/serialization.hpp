#pragma once

#include "io/network/endpoint.hpp"
#include "slk/serialization.hpp"

namespace slk {

inline void Save(const io::network::Endpoint &endpoint, slk::Builder *builder) {
  slk::Save(endpoint.address_, builder);
  slk::Save(endpoint.port_, builder);
  slk::Save(endpoint.family_, builder);
}

inline void Load(io::network::Endpoint *endpoint, slk::Reader *reader) {
  slk::Load(&endpoint->address_, reader);
  slk::Load(&endpoint->port_, reader);
  slk::Load(&endpoint->family_, reader);
}

}  // namespace slk
