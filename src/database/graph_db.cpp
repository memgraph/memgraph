
#include <storage/edge.hpp>
#include "database/creation_exception.hpp"
#include "database/graph_db.hpp"
#include "snapshot/snapshoter.hpp"

#include "storage/vertex.hpp"
#include "storage/vertex_accessor.hpp"
#include "storage/edge.hpp"
#include "storage/edge_accessor.hpp"

GraphDb::GraphDb(bool import_snapshot) : GraphDb("default", import_snapshot) {}

GraphDb::GraphDb(const std::string &name, bool import_snapshot)
    : GraphDb(name.c_str(), import_snapshot) {
}

//GraphDb::GraphDb(const char *name, bool import_snapshot) : name_(name) {
//  if (import_snapshot) snap_engine.import();
//}

