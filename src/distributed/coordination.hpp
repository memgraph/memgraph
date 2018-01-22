#pragma once

#include "io/network/endpoint.hpp"

namespace distributed {

/** API for the distributed coordination class. */
class Coordination {
 public:
  virtual ~Coordination() {}

  /** Gets the endpoint for the given worker ID from the master. */
  virtual io::network::Endpoint GetEndpoint(int worker_id) = 0;
};

}  // namespace distributed
