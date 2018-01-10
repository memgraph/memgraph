#include "gtest/gtest.h"

#include "communication/messaging/distributed.hpp"
#include "database/counters.hpp"

const std::string kLocal = "127.0.0.1";

TEST(CountersDistributed, All) {
  communication::messaging::System master_sys(kLocal, 0);
  database::MasterCounters master(master_sys);

  communication::messaging::System w1_sys(kLocal, 0);
  database::WorkerCounters w1(w1_sys, master_sys.endpoint());

  communication::messaging::System w2_sys(kLocal, 0);
  database::WorkerCounters w2(w2_sys, master_sys.endpoint());

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
