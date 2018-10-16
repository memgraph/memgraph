#pragma once

#include "distributed/coordination.hpp"

namespace database {
class Worker;
};  // namespace database

namespace distributed {

class DurabilityRpcWorker {
 public:
  DurabilityRpcWorker(database::Worker *db, distributed::Coordination *coordination);

 private:
  database::Worker *db_;
};

}  // namespace distributed
