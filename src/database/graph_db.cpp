
#include <storage/edge.hpp>
#include "database/creation_exception.hpp"
#include "database/graph_db.hpp"
//#include "snapshot/snapshoter.hpp"

GraphDb::GraphDb(const std::string &name, bool import_snapshot) : name_(name) {
//  if (import_snapshot)
//    snap_engine.import();
}
