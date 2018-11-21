#pragma once

#include <netinet/in.h>
#include <cstdint>
#include <iostream>
#include <string>

// TODO: SLK serialization should be its own thing
#include "communication/rpc/serialization.hpp"
#include "io/network/endpoint.capnp.h"
#include "utils/exceptions.hpp"

namespace io::network {

/**
 * This class represents a network endpoint that is used in Socket.
 * It is used when connecting to an address and to get the current
 * connection address.
 */
class Endpoint {
 public:
  Endpoint();
  Endpoint(const std::string &address, uint16_t port);

  // TODO: Remove these since members are public
  std::string address() const { return address_; }
  uint16_t port() const { return port_; }
  unsigned char family() const { return family_; }

  bool operator==(const Endpoint &other) const;
  friend std::ostream &operator<<(std::ostream &os, const Endpoint &endpoint);

  std::string address_;
  uint16_t port_{0};
  unsigned char family_{0};
};

void Save(const Endpoint &endpoint, capnp::Endpoint::Builder *builder);

void Load(Endpoint *endpoint, const capnp::Endpoint::Reader &reader);

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
