#include "gtest/gtest.h"

#include "communication/rpc/server.hpp"
#include "database/counters.hpp"

const std::string kLocal = "127.0.0.1";

TEST(CountersDistributed, All) {
  communication::rpc::Server master_server({kLocal, 0});
  database::MasterCounters master(master_server);

  database::WorkerCounters w1(master_server.endpoint());
  database::WorkerCounters w2(master_server.endpoint());

  EXPECT_EQ(w1.Get("a"), 0);
  EXPECT_EQ(w1.Get("a"), 1);
  EXPECT_EQ(w2.Get("a"), 2);
  EXPECT_EQ(w1.Get("a"), 3);
  EXPECT_EQ(master.Get("a"), 4);

  EXPECT_EQ(master.Get("b"), 0);
  EXPECT_EQ(w2.Get("b"), 1);
  w1.Set("b", 42);
  EXPECT_EQ(w2.Get("b"), 42);
}
