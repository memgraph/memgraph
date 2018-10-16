#pragma once

#include "distributed/coordination.hpp"

namespace database {
class GraphDb;
}

namespace distributed {

class IndexRpcServer {
 public:
  IndexRpcServer(database::GraphDb *db, distributed::Coordination *coordination);

 private:
  database::GraphDb *db_;
};

}  // namespace distributed
