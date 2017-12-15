#include <experimental/optional>

#include "gtest/gtest.h"

#include "communication/messaging/distributed.hpp"
#include "database/graph_db_datatypes.hpp"
#include "storage/concurrent_id_mapper_master.hpp"
#include "storage/concurrent_id_mapper_worker.hpp"

using namespace communication::messaging;
using namespace storage;
using namespace GraphDbTypes;

template <typename TId>
class DistributedConcurrentIdMapperTest : public ::testing::Test {
  const std::string kLocal{"127.0.0.1"};

 protected:
  System master_system_{kLocal, 0};
  std::experimental::optional<MasterConcurrentIdMapper<TId>> master_mapper_;
  System worker_system_{kLocal, 0};
  std::experimental::optional<WorkerConcurrentIdMapper<TId>> worker_mapper_;

  void SetUp() override {
    master_mapper_.emplace(master_system_);
    worker_mapper_.emplace(worker_system_, master_system_.endpoint());
  }
  void TearDown() override {
    worker_mapper_ = std::experimental::nullopt;
    worker_system_.Shutdown();
    master_mapper_ = std::experimental::nullopt;
    master_system_.Shutdown();
  }
};

using namespace GraphDbTypes;
typedef ::testing::Types<Label, EdgeType, Property> GraphDbTestTypes;
TYPED_TEST_CASE(DistributedConcurrentIdMapperTest, GraphDbTestTypes);

TYPED_TEST(DistributedConcurrentIdMapperTest, Basic) {
  auto &master = this->master_mapper_.value();
  auto &worker = this->worker_mapper_.value();

  auto id1 = master.value_to_id("v1");
  EXPECT_EQ(worker.id_to_value(id1), "v1");
  EXPECT_EQ(worker.value_to_id("v1"), id1);

  auto id2 = worker.value_to_id("v2");
  EXPECT_EQ(master.id_to_value(id2), "v2");
  EXPECT_EQ(master.value_to_id("v2"),id2);

  EXPECT_NE(id1, id2);
}
