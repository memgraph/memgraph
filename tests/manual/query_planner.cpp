#include "interactive_planning.hpp"

#include <gflags/gflags.h>

#include "storage/v2/storage.hpp"

DECLARE_int32(min_log_level);

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  spdlog::set_level(spdlog::level::err);
  storage::Storage db;
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  RunInteractivePlanning(&dba);
  return 0;
}
