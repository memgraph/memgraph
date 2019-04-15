#include "interactive_planning.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "database/single_node/graph_db.hpp"
#include "database/single_node/graph_db_accessor.hpp"

DECLARE_int32(min_log_level);

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_min_log_level = google::ERROR;
  google::InitGoogleLogging(argv[0]);
  database::GraphDb db;
  auto dba = db.Access();
  RunInteractivePlanning(&dba);
  return 0;
}
