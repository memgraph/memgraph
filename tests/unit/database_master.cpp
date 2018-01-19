#include "gtest/gtest.h"

#include "config.hpp"
#include "database/graph_db.hpp"

TEST(DatabaseMaster, Instantiate) {
  database::Config config;
  config.master_endpoint = io::network::Endpoint("127.0.0.1", 0);
  config.worker_id = 0;
  database::Master master(config);
}
