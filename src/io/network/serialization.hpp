#pragma once

#include "io/network/endpoint.capnp.h"
#include "io/network/endpoint.hpp"
#include "slk/serialization.hpp"

namespace io::network {

inline void Save(const Endpoint &endpoint, capnp::Endpoint::Builder *builder) {
  builder->setAddress(endpoint.address());
  builder->setPort(endpoint.port());
  builder->setFamily(endpoint.family());
}

inline void Load(Endpoint *endpoint, const capnp::Endpoint::Reader &reader) {
  endpoint->address_ = reader.getAddress();
  endpoint->port_ = reader.getPort();
  endpoint->family_ = reader.getFamily();
}

}  // namespace io::network

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
