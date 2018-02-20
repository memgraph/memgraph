#pragma once

#include "io/network/endpoint.hpp"

namespace distributed {

/** API for the distributed coordination class. */
class Coordination {
 public:
  virtual ~Coordination() {}

  /** Gets the endpoint for the given worker ID from the master. */
  virtual io::network::Endpoint GetEndpoint(int worker_id) = 0;

  /** Gets the connected worker ids - should only be called on a master
   * instance*/
  virtual std::vector<int> GetWorkerIds() const = 0;
};

}  // namespace distributed
